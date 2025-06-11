import json
import pymongo
import sys
import traceback

def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        traceback.print_exc()
        return None

def test_mongodb_connection():
    """Test connection to MongoDB"""
    print("Testing MongoDB connection...")
    
    config = load_config()
    if not config:
        print("Failed to load configuration")
        return False
    
    try:
        # Connect to MongoDB
        connection_string = config["mongodb"]["connection_string"]
        print(f"Connecting to MongoDB Atlas using: {connection_string}")
        
        # Set a longer timeout for Atlas connection
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=10000)
        
        # Force a command to check the connection
        print("Attempting to ping MongoDB server...")
        client.admin.command('ping')
        
        print("MongoDB Atlas connection successful!")
        
        # Get database information
        db_name = config["mongodb"]["database"]
        collection_name = config["mongodb"]["collection"]
        
        print(f"Database: {db_name}")
        print(f"Collection: {collection_name}")
        
        # Check if database exists
        print("Listing available databases...")
        db_list = client.list_database_names()
        print(f"Available databases: {db_list}")
        
        if db_name in db_list:
            print(f"Database '{db_name}' exists")
        else:
            print(f"Database '{db_name}' does not exist yet (will be created when data is inserted)")
        
        # Check if collection exists
        db = client[db_name]
        print("Listing available collections...")
        collection_list = db.list_collection_names()
        print(f"Available collections: {collection_list}")
        
        if collection_name in collection_list:
            print(f"Collection '{collection_name}' exists")
            
            # Count documents in collection
            count = db[collection_name].count_documents({})
            print(f"Collection contains {count} documents")
        else:
            print(f"Collection '{collection_name}' does not exist yet (will be created when data is inserted)")
        
        return True
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        print("Full error details:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_mongodb_connection()
    if not success:
        sys.exit(1) 