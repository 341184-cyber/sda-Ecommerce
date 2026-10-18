import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce_streaming")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "orders")

if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in the .env file")



client = MongoClient(
    MONGO_URI,
    tlsCAFile=certifi.where()
)

db = client[MONGO_DB]
orders_collection = db[MONGO_COLLECTION]

def test_connection():
    try:
        client.admin.command("ping")
        print("MongoDB connection successful!")
        print(f"Database: {MONGO_DB}")
        print(f"Collection: {MONGO_COLLECTION}")
        return True
    except Exception as e:
        print("MongoDB connection failed.")
        print(e)
        return False