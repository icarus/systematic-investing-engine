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
        # Query for 1 page
        response = client.databases.query(database_id=db_id, page_size=1)
        results = response.get("results", [])
        if results:
            props = results[0]["properties"]
            print("Found properties in row:", list(props.keys()))
            # Print types too to be sure
            for k, v in props.items():
                print(f"  {k}: {v['type']}")
        else:
            print("No rows found (empty database).")
    except Exception as e:
        print(f"Error: {e}")
