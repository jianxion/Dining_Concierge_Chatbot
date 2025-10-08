import os
import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection

from dotenv import load_dotenv
load_dotenv()


REGION = os.getenv("AWS_REGION", "us-east-1")
OS_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT")
TABLE = os.getenv("DDB_TABLE", "yelp-restaurants")
OS_USERNAME = os.getenv("OPENSEARCH_USERNAME")
OS_PASSWORD = os.getenv("OPENSEARCH_PASSWORD")


os_client = OpenSearch(
    hosts=[{"host": OS_ENDPOINT.replace("https://", ""), "port": 443}],
    http_auth=(OS_USERNAME, OS_PASSWORD),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30,
)

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE)

def ensure_index():
    if not os_client.indices.exists(index="restaurants"):
        os_client.indices.create(
            index="restaurants",
            body={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "properties": {
                        "RestaurantID": {"type":"keyword"},
                        "Cuisine": {"type":"keyword"},
                        "docType": {"type":"keyword"}
                    }
                }
            }
        )

def scan_dynamodb():
    items = []
    resp = table.scan(ProjectionExpression="business_id, cuisine")
    items.extend(resp.get("Items", []))
    return items

def bulk_index(items):
    actions = []
    for it in items:
        doc = {
            "RestaurantID": it.get("business_id"),
            "Cuisine": (it.get("cuisine") or "").lower(),
            "docType": "Restaurant"
        }
        actions.append(json.dumps({ "index": { "_index": "restaurants", "_id": doc["RestaurantID"] }}))
        actions.append(json.dumps(doc))

    if actions:
        payload = "\n".join(actions) + "\n"
        res = os_client.bulk(body=payload)
        if res.get("errors"):
            print("Bulk had errors (final):", json.dumps(res)[:500])

if __name__ == "__main__":
    ensure_index()
    items = scan_dynamodb()
    print(f"Loaded {len(items)} items from DynamoDB.")
    bulk_index(items)
    print("Done.")
