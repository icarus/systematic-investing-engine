import os
from dotenv import load_dotenv
from notion_client import Client

load_dotenv()
token = os.getenv("NOTION_TOKEN")
client = Client(auth=token)

db_ids = {
    "runs": "ebc567f5-2b9c-48ee-b906-891ecf7230af",
    "signals": "7eb2f691-139c-4c74-9d30-fe4bd23346bd",
    "portfolio": "8f236ef2-ccf4-4a39-9f5b-12f5f7aabc67"
}

for name, db_id in db_ids.items():
    print(f"--- {name} ({db_id}) ---")
    try:
        db = client.databases.retrieve(database_id=db_id)
        if "properties" in db:
            print("Properties:", list(db["properties"].keys()))
        else:
            print("No properties found.")
            if "data_sources" in db:
                print("Data Sources:", db["data_sources"])
                # Try retrieving the source
                source_id = db["data_sources"][0]["id"]
                print(f"Retrieving source {source_id}...")
                source_db = client.databases.retrieve(database_id=source_id)
                print("Source Properties:", list(source_db["properties"].keys()))
    except Exception as e:
        print(f"Error: {e}")
