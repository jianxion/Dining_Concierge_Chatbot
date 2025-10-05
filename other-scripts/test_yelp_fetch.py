import os
from datetime import datetime, timezone
from decimal import Decimal
import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

YELP_API_KEY = os.environ.get("YELP_API_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
TABLE_NAME = "yelp-restaurants"

# Test with just one cuisine and 5 results
TEST_CUISINE = "italian"
TEST_LIMIT = 5

YELP_URL = "https://api.yelp.com/v3/businesses/search"
HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
LOCATION = "Manhattan, NY"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

def ensure_table():
    """Check if table exists, create if not"""
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.load()
        print(f"✓ Table '{TABLE_NAME}' exists")
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
            print("✓ Table created successfully")
            return table
        raise

def format_address(biz):
    """Format address from Yelp business data"""
    loc = biz.get("location") or {}
    parts = [loc.get("address1"), loc.get("address2"), loc.get("address3")]
    parts = [p for p in parts if p]
    line = ", ".join(parts) if parts else None
    city = loc.get("city")
    state = loc.get("state")
    zipc = loc.get("zip_code")
    full = ", ".join([p for p in [line, city, state, zipc] if p])
    return full, zipc

def shape_item(biz, cuisine):
    """Shape Yelp business data into DynamoDB item format"""
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

def test_fetch_and_insert():
    """Test fetching from Yelp and inserting into DynamoDB"""
    
    print(f"\n{'='*60}")
    print("YELP API TEST - Fetching and Inserting Data")
    print(f"{'='*60}\n")
    
    # Step 1: Verify API key
    if not YELP_API_KEY:
        print("✗ ERROR: YELP_API_KEY not found in environment")
        return
    print(f"✓ API Key loaded (ends with: ...{YELP_API_KEY[-8:]})")
    
    # Step 2: Ensure table exists
    print(f"\nStep 1: Checking DynamoDB table...")
    try:
        table = ensure_table()
    except Exception as e:
        print(f"✗ ERROR creating/accessing table: {e}")
        return
    
    # Step 3: Fetch from Yelp
    print(f"\nStep 2: Fetching {TEST_LIMIT} {TEST_CUISINE} restaurants from Yelp...")
    params = {
        "term": f"{TEST_CUISINE} restaurants",
        "location": LOCATION,
        "limit": TEST_LIMIT,
        "offset": 0,
        "sort_by": "best_match"
    }
    
    try:
        r = requests.get(YELP_URL, headers=HEADERS, params=params, timeout=15)
        
        if r.status_code != 200:
            print(f"✗ Yelp API error {r.status_code}: {r.text[:200]}")
            return
        
        data = r.json()
        businesses = data.get("businesses", [])
        
        if not businesses:
            print("✗ No businesses returned from Yelp")
            return
        
        print(f"✓ Successfully fetched {len(businesses)} restaurants")
        
    except Exception as e:
        print(f"✗ ERROR fetching from Yelp: {e}")
        return
    
    # Step 4: Display fetched data
    print(f"\nFetched Restaurants:")
    print("-" * 60)
    for i, biz in enumerate(businesses, 1):
        print(f"{i}. {biz.get('name')}")
        print(f"   ID: {biz.get('id')}")
        print(f"   Rating: {biz.get('rating')} ({biz.get('review_count')} reviews)")
        loc = biz.get('location', {})
        print(f"   Address: {loc.get('address1', 'N/A')}, {loc.get('city', 'N/A')}")
        print()
    
    # Step 5: Insert into DynamoDB
    print(f"Step 3: Inserting into DynamoDB table '{TABLE_NAME}'...")
    inserted_count = 0
    
    try:
        with table.batch_writer(overwrite_by_pkeys=["business_id"]) as batch:
            for biz in businesses:
                item = shape_item(biz, TEST_CUISINE)
                batch.put_item(Item=item)
                inserted_count += 1
        
        print(f"✓ Successfully inserted {inserted_count} restaurants")
        
    except Exception as e:
        print(f"✗ ERROR inserting into DynamoDB: {e}")
        return
    
    # Step 6: Verify data in DynamoDB
    print(f"\nStep 4: Verifying data in DynamoDB...")
    try:
        # Read back one item to verify
        first_biz_id = businesses[0].get('id')
        response = table.get_item(Key={'business_id': first_biz_id})
        
        if 'Item' in response:
            item = response['Item']
            print(f"✓ Verified - Sample item retrieved:")
            print(f"  Business ID: {item.get('business_id')}")
            print(f"  Name: {item.get('name')}")
            print(f"  Rating: {item.get('rating')}")
            print(f"  Review Count: {item.get('review_count')}")
            print(f"  Zip Code: {item.get('zip_code')}")
            print(f"  Coordinates: lat={item.get('coordinates', {}).get('lat')}, lon={item.get('coordinates', {}).get('lon')}")
        else:
            print("✗ Could not verify - item not found")
            
    except Exception as e:
        print(f"✗ ERROR verifying data: {e}")
    
    print(f"\n{'='*60}")
    print("TEST COMPLETE!")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    test_fetch_and_insert()
