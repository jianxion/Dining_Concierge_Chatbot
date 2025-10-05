import json
import os
import random
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = "us-east-1"
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
OS_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].replace("https://", "")
DDB_TABLE = os.environ["DDB_TABLE"]
SES_FROM = os.environ["SES_FROM"]
HITS_PER_EMAIL = int(os.getenv("HITS_PER_EMAIL", "3"))


session = boto3.Session(region_name=REGION)
credentials = session.get_credentials().get_frozen_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, "es", session_token=credentials.token)

os_client = OpenSearch(
    hosts=[{"host": OS_ENDPOINT, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30,
)

sqs = boto3.client("sqs", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION).Table(DDB_TABLE)
ses = boto3.client("ses", region_name=REGION)

def _receive_messages(max_msgs=2, visibility_timeout=30, wait_time=10):
    resp = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=max_msgs,
        VisibilityTimeout=visibility_timeout,
        WaitTimeSeconds=wait_time
    )
    return resp.get("Messages", [])

def _delete_message(receipt_handle):
    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)


# search OpenSearch for random restaurants by cuisine
def _search_random_by_cuisine(cuisine, size=10):
    """
    Pick random hits via OpenSearch function_score + random_score,
    filtered by Cuisine keyword.
    """
    seed = random.randint(1, 1_000_000)
    body = {
        "size": size,
        "query": {
            "function_score": {
                "query": {"term": {"Cuisine": cuisine.lower()}},
                "random_score": {"seed": seed}
            }
        }
    }
    r = os_client.search(index="restaurants", body=body)
    hits = r.get("hits", {}).get("hits", [])
    # Each _source has RestaurantID and Cuisine
    return [h["_source"] for h in hits]

def _get_ddb_details(business_ids):
    """
    Fetch details from DynamoDB for the selected business IDs.
    Optimized for small number of records (typically 3-5 for recommendations).
    Required fields: Business ID, Name, Address, Coordinates, Number of Reviews, Rating, Zip Code
    """
    if not business_ids:
        return []
    
    results = []
    dynamo = boto3.client("dynamodb", region_name=REGION)
    
    # For small number of records, single batch_get_item is sufficient
    keys = [{"business_id": {"S": bid}} for bid in business_ids]
    
    try:
        resp = dynamo.batch_get_item(
            RequestItems={
                DDB_TABLE: {
                    "Keys": keys,
                    "ProjectionExpression": "business_id, #n, address, coordinates, review_count, rating, zip_code",
                    "ExpressionAttributeNames": {"#n": "name"}
                }
            }
        )
        items = resp.get("Responses", {}).get(DDB_TABLE, [])
        
        # Convert DynamoDB AttributeValue format to Python dicts
        for av in items:
            obj = {
                "business_id": av["business_id"]["S"],
                "name": av.get("name", {}).get("S"),
                "address": av.get("address", {}).get("S"),
                "coordinates": {
                    "lat": float(av["coordinates"]["M"]["lat"]["N"]) if "coordinates" in av and "lat" in av["coordinates"]["M"] else None,
                    "lon": float(av["coordinates"]["M"]["lon"]["N"]) if "coordinates" in av and "lon" in av["coordinates"]["M"] else None,
                } if "coordinates" in av else None,
                "review_count": int(av["review_count"]["N"]) if "review_count" in av else None,
                "rating": float(av["rating"]["N"]) if "rating" in av else None,
                "zip_code": av.get("zip_code", {}).get("S")
            }
            results.append(obj)
        
        # Preserve input order
        order = {bid: idx for idx, bid in enumerate(business_ids)}
        results.sort(key=lambda x: order.get(x["business_id"], 999999))
        
    except ClientError as e:
        print(f"Error fetching from DynamoDB: {e}")
        return []
    
    return results



def _format_email_html(cuisine, recs):
    rows = []
    for r in recs:
        name = r.get("name") or r["business_id"]
        addr = r.get("address") or "—"
        rating = r.get("rating")
        reviews = r.get("review_count")
        zipc = r.get("zip_code") or "—"
        coords = r.get("coordinates") or {}
        lat = coords.get("lat"); lon = coords.get("lon")
        map_link = f"https://www.google.com/maps?q={lat},{lon}" if lat and lon else "#"
        rows.append(f"""
          <tr>
            <td style="padding:8px 12px;border-bottom:1px solid #eee;"><b>{name}</b><br/>
              <span style="color:#555">{addr}</span><br/>
              <span>Rating: {rating} ⭐ ({reviews} reviews) | Zip: {zipc}</span><br/>
              <a href="{map_link}">Map</a>
            </td>
          </tr>
        """)
    body = f"""
      <div style="font-family:Arial,Helvetica,sans-serif">
        <p>Here are {len(recs)} {cuisine.title()} restaurant suggestion(s) in Manhattan:</p>
        <table style="border-collapse:collapse;width:100%;">{''.join(rows)}</table>
        <p style="color:#888;font-size:12px;margin-top:16px;">Sent {datetime.now(timezone.utc).isoformat()}</p>
      </div>
    """
    return body

def _send_email(to_email, subject, html_body):
    ses.send_email(
        Source=SES_FROM,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": html_body, "Charset": "UTF-8"}}
        }
    )


def process_one_message(msg):
    """
    Message body (from LF1) example:
    {
      "location": "Manhattan",
      "cuisine": "italian",
      "dining_time": "19:30",
      "party_size": 2,
      "email": "you@example.com",
      "requested_at_iso": "..."
    }
    """
    payload = json.loads(msg["Body"])
    cuisine = (payload.get("cuisine") or "").lower()
    to_email = payload["email"]

    # Search OpenSearch for random docs of this cuisine, then pick top N
    candidate_sources = _search_random_by_cuisine(cuisine, size=max(20, HITS_PER_EMAIL*4))
    if not candidate_sources:
        _send_email(to_email, f"No {cuisine.title()} suggestions right now", f"Sorry—couldn't find any {cuisine} results.")
        return True

    # Pick unique RestaurantIDs keeping randomness
    ids = []
    seen = set()
    random.shuffle(candidate_sources)
    for s in candidate_sources:
        rid = s.get("RestaurantID")
        if rid and rid not in seen:
            seen.add(rid)
            ids.append(rid)
        if len(ids) >= HITS_PER_EMAIL:
            break

    # Enrich from DynamoDB
    details = _get_ddb_details(ids)
    if not details:
        _send_email(to_email, f"{cuisine.title()} suggestions", "We had trouble fetching details—please try again.")
        return True

    html = _format_email_html(cuisine, details)
    subject = f"{cuisine.title()} picks for you"
    _send_email(to_email, subject, html)
    return True


def lambda_handler(event, context):
    """
    Invoked by EventBridge (every minute).
    Pull up to 10 messages, process each, delete on success.
    """
    msgs = _receive_messages(max_msgs=2, visibility_timeout=60, wait_time=0)
    if not msgs:
        print("No messages to process.")
        return {"ok": True, "processed": 0}

    processed = 0
    for m in msgs:
        try:
            ok = process_one_message(m)
            if ok:
                _delete_message(m["ReceiptHandle"])
                processed += 1
        except Exception as e:
            # Leave message in queue; visibility timeout will expire → will be retried
            print("Error processing message:", e)
    return {"ok": True, "processed": processed}