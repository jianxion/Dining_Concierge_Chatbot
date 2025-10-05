import json
import os
import uuid
import boto3
import hashlib
from datetime import datetime, timezone


lex = boto3.client("lexv2-runtime", region_name="us-east-1")

BOT_ID = os.environ.get("LEX_BOT_ID")
ALIAS_ID = os.environ.get("LEX_BOT_ALIAS_ID")
LOCALE_ID = os.getenv("LEX_LOCALE_ID", "en_US")

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "OPTIONS,POST"
}



def _derive_session_id(event):
    rc = event.get("requestContext", {}) or {}
    ip = (rc.get("identity") or {}).get("sourceIp") or "0.0.0.0"
    ua = (event.get("headers") or {}).get("User-Agent") or (event.get("headers") or {}).get("user-agent") or "unknown"
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    raw = f"{ip}|{ua}|{day}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _ok(body):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", **CORS_HEADERS},
        "body": json.dumps(body)
    }

def _err(status, msg):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", **CORS_HEADERS},
        "body": json.dumps({"code": status, "message": msg})
    }

def lambda_handler(event, context):
    # Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 204, "headers": CORS_HEADERS, "body": ""}

    try:
        payload = json.loads(event.get("body") or "{}")
    except Exception:
        return _err(400, "Invalid JSON body")

    # Extract user text from BotRequest
    user_text = ""
    if isinstance(payload.get("messages"), list) and payload["messages"]:
        first = payload["messages"][0]
        user_text = (first.get("unstructured", {}).get("text") or "").strip()

    if not user_text:
        return _err(400, "Missing message text in BotRequest")

    # session_id = payload.get("sessionId") or str(uuid.uuid4())[:100]
    session_id = (payload.get("sessionId") or _derive_session_id(event))[:100]
    print(f"Session ID: {session_id}")

    try:
        resp = lex.recognize_text(
            botId=BOT_ID,
            botAliasId=ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=user_text
        )
    except Exception as e:
        print("Lex error:", str(e))
        return _err(502, f"Lex error: {str(e)}")

    # Map Lex messages into BotResponse.messages[]
    lex_messages = resp.get("messages") or []
    response_messages = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for m in lex_messages:
        if not m.get("content"):
            continue
        response_messages.append({
            "type": "unstructured",
            "unstructured": {
                "id": str(uuid.uuid4()),
                "text": m["content"],
                "timestamp": now_iso
            }
        })

    # If Lex returned nothing, still send one placeholder message
    if not response_messages:
        response_messages.append({
            "type": "unstructured",
            "unstructured": {
                "id": str(uuid.uuid4()),
                "text": "…",
                "timestamp": now_iso
            }
        })

    return _ok({"messages": response_messages})