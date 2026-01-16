import os
import asyncio
import argparse
from azure.storage.blob.aio import BlobServiceClient

async def reset_storage(conn_str: str, container_name: str, processed_prefix: str):
    """Deletes all .done marker files in the processed prefix."""
    print(f"Using Container: {container_name}")
    print(f"Searching for markers starting with: '{processed_prefix}'")
    
    async with BlobServiceClient.from_connection_string(conn_str) as client:
        container = client.get_container_client(container_name)
        
        count = 0
        try:
            async for blob in container.list_blobs(name_starts_with=processed_prefix):
                if blob.name.endswith(".done"):
                    print(f"Deleting marker: {blob.name}")
                    await container.delete_blob(blob.name)
                    count += 1
        except Exception as e:
            print(f"Error accessing blobs: {e}")
            return
        
        print(f"Reset complete. {count} markers deleted.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset preprocessor state by deleting .done markers.")
    parser.add_argument("--container", default=os.getenv("BLOB_CONTAINER", "raw-logs"), help="Storage container name")
    parser.add_argument("--prefix", default=os.getenv("PROCESSED_PREFIX", "processed/"), help="Prefix for marker files")
    args = parser.parse_args()

    conn = os.getenv("BLOB_CONN")
    if not conn:
        print("Error: BLOB_CONN environment variable is not set.")
    else:
        asyncio.run(reset_storage(conn, args.container, args.prefix))
