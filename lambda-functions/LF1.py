import json
import os
import re
import boto3
from datetime import datetime


# --- Config ---
# Prefer setting this via Lambda environment variables
SQS_QUEUE_URL = os.getenv(
    "SQS_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/298156079577/DiningConciergeQueue"
)

sqs = boto3.client("sqs")

# ---------- Lex V2 helpers ----------
def response(session_state, messages=None, session_attributes=None):
    body = {"sessionState": session_state}
    if session_attributes:
        body["sessionState"]["sessionAttributes"] = session_attributes
    if messages:
        body["messages"] = messages
    return body

def text_message(content):
    return {"contentType": "PlainText", "content": content}

def get_slots(intent):
    # Lex V2 event: event['sessionState']['intent']['slots']
    return intent.get("slots", {})

def get_slot_value(slots, name):
    """
    Lex V2 slot format:
    slots[name] -> { "value": { "originalValue": "...", "interpretedValue": "...", ... } }
    """
    s = slots.get(name)
    if not s:
        return None
    value = s.get("value", {})
    # Prefer interpretedValue (normalized by Lex); fall back to originalValue when missing
    return value.get("interpretedValue") or value.get("originalValue")

def elicit_slot(event, slot_to_elicit, message):
    intent = event["sessionState"]["intent"]
    return response(
        session_state={
            "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot_to_elicit},
            "intent": intent
        },
        messages=[text_message(message)]
    )

def delegate(event):
    intent = event["sessionState"]["intent"]
    return response(
        session_state={
            "dialogAction": {"type": "Delegate"},
            "intent": intent
        }
    )

def close(event, fulfillment_state, message):
    # fulfillment_state: "Fulfilled" | "Failed"
    intent = event["sessionState"]["intent"]
    intent["state"] = fulfillment_state
    return response(
        session_state={
            "dialogAction": {"type": "Close"},
            "intent": intent
        },
        messages=[text_message(message)]
    )

# ---------- Validation ----------
CUISINE_SET = {
    "american","chinese","indian","italian","japanese","korean",
    "mexican","thai","french","mediterranean","vietnamese"
}

def is_valid_location(s):
    # Simple non-empty check; customize to whitelist cities/regions if needed
    return bool(s and len(s.strip()) >= 2)

def is_valid_cuisine(s):
    return s and s.lower() in CUISINE_SET

def is_valid_time_hhmm(s):
    # s may be like 'T02:20:00', '02:20:00', '2:20', '02:20', etc.
    print("DEBUG DiningTime slot:", s)
    if not s:
        return False
    s = s.strip()
    # strip leading 'T' from AMAZON.Time values
    if s.startswith('T'):
        s = s[1:]

    # If seconds are present, drop them (e.g., '02:20:00' -> '02:20')
    if re.match(r'^\d{1,2}:\d{2}:\d{2}$', s):
        s = s.rsplit(':', 1)[0]

    # Accept 'H:MM' or 'HH:MM'
    if not re.match(r'^\d{1,2}:\d{2}$', s):
        return False
    try:
        h, m = map(int, s.split(':'))
        return 0 <= h <= 23 and 0 <= m <= 59
    except (ValueError, TypeError):
        return False

def is_valid_party_size(s):
    try:
        n = int(s)
        return 1 <= n <= 20
    except Exception:
        return False

def is_valid_email(s):
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s or ""))

def validate_slots(slots):
    location = get_slot_value(slots, "Location")
    cuisine = get_slot_value(slots, "Cuisine")
    dining_time = get_slot_value(slots, "DiningTime")
    party_size = get_slot_value(slots, "PartySize")
    email = get_slot_value(slots, "Email")

    if location and not is_valid_location(location):
        return False, "Location", "Which city are you dining in?"

    if cuisine and not is_valid_cuisine(cuisine.lower()):
        return False, "Cuisine", f"I can help with {', '.join(sorted(CUISINE_SET))}. Which cuisine would you like?"

    # If you used AMAZON.Time and receive values like "T19:30",
    # you can normalize here (strip leading 'T' and optional seconds if present).
    if dining_time:
        normalized = dining_time.strip()
        if normalized.startswith('T'):
            normalized = normalized[1:]
        # drop seconds if present
        if re.match(r'^\d{1,2}:\d{2}:\d{2}$', normalized):
            normalized = normalized.rsplit(':', 1)[0]
        # store back the normalized value so interpretedValue is HH:MM
        try:
            slots["DiningTime"]["value"]["interpretedValue"] = normalized
        except Exception:
            pass

        if not is_valid_time_hhmm(normalized):
            return False, "DiningTime", "At what time? Please use 24h HH:MM (e.g., 19:30)."

    if party_size and not is_valid_party_size(party_size):
        return False, "PartySize", "How many people? (1–20)"

    if email and not is_valid_email(email):
        return False, "Email", "What email should I send suggestions to?"

    return True, None, None

# ---------- Intent handlers ----------
def handle_greeting(event):
    return close(event, "Fulfilled", "Hi there, how can I help?")

def handle_thank_you(event):
    return close(event, "Fulfilled", "You’re welcome! Happy to help.")

def handle_dining_suggestions(event):
    invocation = event.get("invocationSource")  # "DialogCodeHook" or "FulfillmentCodeHook"
    intent = event["sessionState"]["intent"]
    slots = get_slots(intent)

    # -------- Dialog phase: validate + elicit missing slots --------
    if invocation == "DialogCodeHook":
        ok, bad_slot, msg = validate_slots(slots)
        if not ok:
            return elicit_slot(event, bad_slot, msg)

        # Elicit any remaining required slots (if Lex hasn't filled them yet)
        needed = [
            ("Location", "Which city are you dining in?"),
            ("Cuisine", f"What cuisine? (e.g., {', '.join(sorted(CUISINE_SET))})"),
            ("DiningTime", "What time? Please use 24h HH:MM (e.g., 19:30)."),
            ("PartySize", "For how many people?"),
            ("Email", "What email should I send suggestions to?")
        ]
        for slot_name, prompt in needed:
            if not get_slot_value(slots, slot_name):
                return elicit_slot(event, slot_name, prompt)

        # All good—let Lex proceed to fulfillment
        return delegate(event)

    # -------- Fulfillment phase: push message to SQS and confirm --------
    location = get_slot_value(slots, "Location")
    cuisine = get_slot_value(slots, "Cuisine")
    dining_time = get_slot_value(slots, "DiningTime")
    party_size = get_slot_value(slots, "PartySize")
    email = get_slot_value(slots, "Email")

    # Normalize/convert types
    cuisine = cuisine.lower() if cuisine else cuisine
    party_size_int = int(party_size) if party_size else None

    payload = {
        "location": location,
        "cuisine": cuisine,
        "dining_time": dining_time,      # "HH:MM"
        "party_size": party_size_int,
        "email": email,
        "requested_at_iso": datetime.utcnow().isoformat() + "Z"
    }

    # Send to SQS
    sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(payload)
    )

    confirm = (
        f"Got it! I’ll email {email} a list of {cuisine} places in {location} "
        f"for around {dining_time} for {party_size} people."
    )
    return close(event, "Fulfilled", confirm)

# ---------- Entrypoint ----------
def lambda_handler(event, context):
    # Uncomment for debugging:
    # import logging; logging.getLogger().setLevel(logging.INFO)
    # print(json.dumps(event, indent=2))

    intent_name = event["sessionState"]["intent"]["name"]

    if intent_name == "GreetingIntent":
        return handle_greeting(event)
    if intent_name == "ThankYouIntent":
        return handle_thank_you(event)
    if intent_name == "DiningSuggestionsIntent":
        return handle_dining_suggestions(event)

    return close(event, "Failed", "Sorry, I didn’t get that.")