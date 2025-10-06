import boto3
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
import requests
from botocore.exceptions import ClientError

from dotenv import load_dotenv
load_dotenv()

YELP_API_KEY = os.environ["YELP_API_KEY"]
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
TABLE_NAME = "yelp-restaurants"

# At least 5 cuisines, target ~200 each
CUISINES = ["american", "chinese", "italian", "japanese", "indian"]

# Yelp constants
YELP_URL = "https://api.yelp.com/v3/businesses/search"
HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
LOCATION = "Manhattan, NY"
LIMIT = 50               # Yelp max per request
PER_CUISINE_TARGET = 200 # ≈200 each -> 4 pages
SLEEP_SEC = 0.35         # throttle to be polite

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
client = boto3.client("dynamodb", region_name=AWS_REGION)

def ensure_table():
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.load()
        return table
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"Creating table {TABLE_NAME}...")
            table = dynamodb.create_table(
                TableName=TABLE_NAME,
                KeySchema=[{"AttributeName":"business_id","KeyType":"HASH"}],
                AttributeDefinitions=[{"AttributeName":"business_id","AttributeType":"S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            table.wait_until_exists()
            print("Table created.")
            return table
        raise

def format_address(biz):
    # Yelp returns address1/2/3, city, state, zip_code
    loc = biz.get("location") or {}
    parts = [loc.get("address1"), loc.get("address2"), loc.get("address3")]
    parts = [p for p in parts if p]
    line = ", ".join(parts) if parts else None
    city = loc.get("city")
    state = loc.get("state")
    zipc = loc.get("zip_code")
    # full address as a single string for convenience
    full = ", ".join([p for p in [line, city, state, zipc] if p])
    return full, zipc

def shape_item(biz, cuisine):
    # Required fields: Business ID, Name, Address, Coordinates, Number of Reviews, Rating, Zip Code
    address, zipc = format_address(biz)
    coords = biz.get("coordinates") or {}
    
    # Convert floats to Decimal for DynamoDB compatibility
    lat = coords.get("latitude")
    lon = coords.get("longitude")
    rating = biz.get("rating")
    
    return {
        "business_id": biz.get("id"),
        "name": biz.get("name"),
        "address": address,
        "coordinates": {
            "lat": Decimal(str(lat)) if lat is not None else None,
            "lon": Decimal(str(lon)) if lon is not None else None
        },
        "review_count": biz.get("review_count"),
        "rating": Decimal(str(rating)) if rating is not None else None,
        "zip_code": zipc,
        "cuisine": cuisine,
        "insertedAtTimestamp": datetime.now(timezone.utc).isoformat()
    }

def batch_write(table, items):
    # DynamoDB batch write 25 at a time
    with table.batch_writer(overwrite_by_pkeys=["business_id"]) as batch:
        for it in items:
            batch.put_item(Item=it)

def fetch_cuisine(cuisine):
    collected = []
    total_seen_ids = set()
    # Try four pages: 0, 50, 100, 150
    for offset in range(0, PER_CUISINE_TARGET, LIMIT):
        params = {
            "term": f"{cuisine} restaurants",
            "location": LOCATION,
            "limit": LIMIT,
            "offset": offset,
            "sort_by": "best_match"  # could randomize with offset patterns if desired
        }
        r = requests.get(YELP_URL, headers=HEADERS, params=params, timeout=15)
        if r.status_code != 200:
            print(f"[{cuisine}] Yelp error {r.status_code}: {r.text}")
            break

        data = r.json()
        businesses = data.get("businesses", [])
        if not businesses:
            break

        for b in businesses:
            bid = b.get("id")
            if not bid or bid in total_seen_ids:
                continue
            total_seen_ids.add(bid)
            collected.append(b)

        # stop early if we already have >= target
        if len(collected) >= PER_CUISINE_TARGET:
            break

        time.sleep(SLEEP_SEC)

    print(f"[{cuisine}] collected {len(collected)} raw businesses")
    return collected

def main():
    table = ensure_table()
    global_seen = set()
    to_write = []

    for cuisine in CUISINES:
        raw = fetch_cuisine(cuisine)
        for biz in raw:
            bid = biz.get("id")
            if not bid or bid in global_seen:
                continue
            global_seen.add(bid)
            item = shape_item(biz, cuisine)
            to_write.append(item)

    print(f"Total unique businesses across cuisines: {len(to_write)}")
    # Write in batches of ~300 to avoid huge single batch (batch_writer handles chunking internally)
    batch_write(table, to_write)
    print("Done. Wrote items to DynamoDB.")

if __name__ == "__main__":
    main()
