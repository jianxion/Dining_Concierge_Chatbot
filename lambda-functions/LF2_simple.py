import json
import os
import random
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
DDB_TABLE = os.environ["DDB_TABLE"]
SES_FROM = os.environ["SES_FROM"]
HITS_PER_EMAIL = int(os.getenv("HITS_PER_EMAIL", "3"))

# AWS clients
sqs = boto3.client("sqs", region_name=REGION)
ddb_client = boto3.client("dynamodb", region_name=REGION)
ddb_resource = boto3.resource("dynamodb", region_name=REGION).Table(DDB_TABLE)
ses = boto3.client("ses", region_name=REGION)


def _receive_messages(max_msgs=2, visibility_timeout=30, wait_time=10):
    """Receive messages from SQS queue"""
    resp = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=max_msgs,
        VisibilityTimeout=visibility_timeout,
        WaitTimeSeconds=wait_time
    )
    return resp.get("Messages", [])


def _delete_message(receipt_handle):
    """Delete a message from SQS after processing"""
    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)


def _scan_restaurants_by_cuisine(cuisine, limit=20):
    """
    Scan DynamoDB for restaurants matching the cuisine.
    Note: This uses Scan which is less efficient than OpenSearch.
    In production, you should use OpenSearch or create a GSI on cuisine.
    """
    try:
        response = ddb_resource.scan(
            FilterExpression="cuisine = :cuisine",
            ExpressionAttributeValues={":cuisine": cuisine.lower()},
            Limit=limit
        )
        items = response.get("Items", [])
        
        # Randomly shuffle for variety
        random.shuffle(items)
        return items[:HITS_PER_EMAIL]
    except ClientError as e:
        print(f"Error scanning DynamoDB: {e}")
        return []


def _get_ddb_details(business_ids):
    """
    Fetch details from DynamoDB for the selected business IDs.
    Optimized for small number of records (typically 3-5 for recommendations).
    Required fields: Business ID, Name, Address, Coordinates, Number of Reviews, Rating, Zip Code
    """
    if not business_ids:
        return []
    
    results = []
    keys = [{"business_id": {"S": bid}} for bid in business_ids]
    
    try:
        resp = ddb_client.batch_get_item(
            RequestItems={
                DDB_TABLE: {
                    "Keys": keys,
                    "ProjectionExpression": "business_id, #n, address, coordinates, review_count, rating, zip_code, cuisine",
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
                "zip_code": av.get("zip_code", {}).get("S"),
                "cuisine": av.get("cuisine", {}).get("S")
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
    """Format restaurant recommendations as HTML email"""
    rows = []
    for r in recs:
        name = r.get("name") or r["business_id"]
        addr = r.get("address") or "—"
        rating = r.get("rating")
        reviews = r.get("review_count")
        zipc = r.get("zip_code") or "—"
        coords = r.get("coordinates") or {}
        lat = coords.get("lat")
        lon = coords.get("lon")
        map_link = f"https://www.google.com/maps?q={lat},{lon}" if lat and lon else "#"
        
        rows.append(f"""
          <tr>
            <td style="padding:8px 12px;border-bottom:1px solid #eee;">
              <b>{name}</b><br/>
              <span style="color:#555">{addr}</span><br/>
              <span>Rating: {rating} ⭐ ({reviews} reviews) | Zip: {zipc}</span><br/>
              <a href="{map_link}" style="color:#007bff;">View on Map</a>
            </td>
          </tr>
        """)
    
    body = f"""
      <div style="font-family:Arial,Helvetica,sans-serif;max-width:600px;margin:0 auto;">
        <h2 style="color:#333;">Your {cuisine.title()} Restaurant Recommendations</h2>
        <p>Here are {len(recs)} {cuisine.title()} restaurant suggestion(s) in Manhattan:</p>
        <table style="border-collapse:collapse;width:100%;border:1px solid #ddd;">
          {''.join(rows)}
        </table>
        <p style="color:#888;font-size:12px;margin-top:16px;">
          Sent at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
        </p>
      </div>
    """
    return body


def _send_email(to_email, subject, html_body):
    """Send email via AWS SES"""
    try:
        ses.send_email(
            Source=SES_FROM,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Html": {"Data": html_body, "Charset": "UTF-8"}}
            }
        )
        print(f"Email sent successfully to {to_email}")
    except ClientError as e:
        print(f"Error sending email: {e}")
        raise


def process_one_message(msg):
    """
    Process a single SQS message containing restaurant recommendation request.
    
    Expected message body format:
    {
      "location": "Manhattan",
      "cuisine": "italian",
      "dining_time": "19:30",
      "party_size": 2,
      "email": "user@example.com",
      "requested_at_iso": "..."
    }
    """
    try:
        payload = json.loads(msg["Body"])
        cuisine = (payload.get("cuisine") or "").lower()
        to_email = payload.get("email")
        
        if not to_email:
            print("No email address provided in message")
            return False
        
        if not cuisine:
            print("No cuisine provided in message")
            _send_email(
                to_email,
                "Missing Information",
                "<p>Sorry, we couldn't process your request. Please specify a cuisine type.</p>"
            )
            return True
        
        print(f"Processing request for {cuisine} cuisine, sending to {to_email}")
        
        # Query DynamoDB for restaurants of this cuisine
        restaurants = _scan_restaurants_by_cuisine(cuisine, limit=20)
        
        if not restaurants:
            print(f"No restaurants found for cuisine: {cuisine}")
            _send_email(
                to_email,
                f"No {cuisine.title()} Restaurants Found",
                f"<p>Sorry, we couldn't find any {cuisine} restaurants at the moment. Please try a different cuisine.</p>"
            )
            return True
        
        # Get detailed information (in this case, scan already returns full items)
        # But keeping this pattern in case you want to use business_ids only from index
        business_ids = [r.get("business_id") for r in restaurants if r.get("business_id")]
        
        if not business_ids:
            print("No valid business IDs found")
            _send_email(
                to_email,
                f"Error with {cuisine.title()} Suggestions",
                "<p>We encountered an error processing your request. Please try again.</p>"
            )
            return True
        
        # Format and send email
        html = _format_email_html(cuisine, restaurants)
        subject = f"Your {cuisine.title()} Restaurant Recommendations"
        _send_email(to_email, subject, html)
        
        print(f"Successfully processed and sent {len(restaurants)} recommendations")
        return True
        
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in message body: {e}")
        return False
    except Exception as e:
        print(f"Error processing message: {e}")
        return False


def lambda_handler(event, context):
    """
    Lambda handler invoked by EventBridge (e.g., every minute).
    Polls SQS queue, processes messages, and deletes successfully processed ones.
    """
    print("Lambda invoked - checking for messages")
    
    # Receive messages from SQS
    msgs = _receive_messages(max_msgs=2, visibility_timeout=60, wait_time=0)
    
    if not msgs:
        print("No messages to process.")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "ok": True,
                "processed": 0,
                "message": "No messages in queue"
            })
        }
    
    print(f"Received {len(msgs)} message(s) from SQS")
    
    processed = 0
    failed = 0
    
    for m in msgs:
        try:
            success = process_one_message(m)
            if success:
                # Delete message only if processing succeeded
                _delete_message(m["ReceiptHandle"])
                processed += 1
                print(f"Successfully processed and deleted message")
            else:
                failed += 1
                print(f"Failed to process message - will retry later")
        except Exception as e:
            # Leave message in queue; visibility timeout will expire and it will be retried
            failed += 1
            print(f"Exception processing message: {e}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "processed": processed,
            "failed": failed,
            "total_received": len(msgs)
        })
    }
