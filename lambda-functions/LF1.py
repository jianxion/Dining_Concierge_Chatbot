import json
import os
from datetime import datetime
import boto3


SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
sqs = boto3.client("sqs")

def text_message(content):
    return {"contentType": "PlainText", "content": content}

def response(session_state, messages=None, session_attributes=None):
    body = {"sessionState": session_state}
    if session_attributes:
        body["sessionState"]["sessionAttributes"] = session_attributes
    if messages:
        body["messages"] = messages
    return body

def close(event, fulfillment_state, message):
    intent = event["sessionState"]["intent"]
    intent["state"] = fulfillment_state  # "Fulfilled" | "Failed"
    return response(
        session_state={
            "dialogAction": {"type": "Close"},
            "intent": intent
        },
        messages=[text_message(message)]
    )

def get_slots(event):
    return (event.get("sessionState", {}).get("intent", {}) or {}).get("slots", {}) or {}

def get_slot_value(slots, name):
    s = slots.get(name)
    val = s and s.get("value", {}).get("interpretedValue")
    return val.strip() if isinstance(val, str) else val


def handle_greeting(event):
    return close(event, "Fulfilled", "Hi there, how can I help?")

def handle_thank_you(event):
    return close(event, "Fulfilled", "You’re welcome! Happy to help.")

def handle_dining_suggestions(event):
    invocation = event.get("invocationSource")  # should be "FulfillmentCodeHook"
    if invocation != "FulfillmentCodeHook":
        # Safety: delegate back to Lex if we ever get called during dialog
        intent = event["sessionState"]["intent"]
        return response(session_state={"dialogAction": {"type": "Delegate"}, "intent": intent})

    slots = get_slots(event)
    location = get_slot_value(slots, "Location")
    cuisine  = get_slot_value(slots, "Cuisine")
    dining_time = get_slot_value(slots, "DiningTime")
    party_size  = get_slot_value(slots, "PartySize")
    email    = get_slot_value(slots, "Email")

    payload = {
        "location": location,
        "cuisine": (cuisine or "").lower(),
        "dining_time": dining_time,
        "party_size": party_size,
        "email": email,
        "requested_at_iso": datetime.utcnow().isoformat() + "Z"
    }

    print("Payload:", payload)

 
    if not SQS_QUEUE_URL:
        return close(event, "Failed", "Configuration error: SQS_QUEUE_URL is not set.")
    sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(payload))

    msg = (f"Got it! I’ll email {email} a list of {payload['cuisine']} places in {location} "
           f"around {dining_time} for {party_size} people.")
    return close(event, "Fulfilled", msg)

def lambda_handler(event, context):
    print(json.dumps(event, indent=2))

    intent_name = event["sessionState"]["intent"]["name"]

    if intent_name == "GreetingIntent":
        return handle_greeting(event)
    if intent_name == "ThankYouIntent":
        return handle_thank_you(event)
    if intent_name == "DiningSuggestionsIntent":
        return handle_dining_suggestions(event)

    return close(event, "Fulfilled", "Sorry, I didn’t catch that.")