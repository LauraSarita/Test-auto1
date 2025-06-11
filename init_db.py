import json
import pymongo
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("init_db.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return None

def init_mongodb():
    """Initialize MongoDB database and collection if they don't exist"""
    config = load_config()
    if not config:
        logger.error("Failed to load configuration")
        return False
    
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(config["mongodb"]["connection_string"])
        
        # Create database if it doesn't exist
        db_name = config["mongodb"]["database"]
        db = client[db_name]
        
        # Create collection if it doesn't exist
        collection_name = config["mongodb"]["collection"]
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            logger.info(f"Created collection '{collection_name}' in database '{db_name}'")
        else:
            logger.info(f"Collection '{collection_name}' already exists in database '{db_name}'")
        
        # Create index on fecha field for faster queries
        collection = db[collection_name]
        collection.create_index("fecha", unique=True)
        logger.info("Created index on 'fecha' field")
        
        logger.info("MongoDB initialization completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing MongoDB: {e}")
        return False

if __name__ == "__main__":
    logger.info("Starting MongoDB initialization")
    success = init_mongodb()
    if success:
        logger.info("MongoDB initialization completed successfully")
    else:
        logger.error("MongoDB initialization failed") 