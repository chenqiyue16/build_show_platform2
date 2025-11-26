"""
Common tasks and database operations for the flask application.
"""
import json
import traceback
from io import BufferedReader
from flask import request, jsonify
from pymongo.errors import ConnectionFailure, PyMongoError, DuplicateKeyError
from pymongo import MongoClient
from werkzeug.utils import secure_filename
import gridfs
from bson import ObjectId
from .project_setting import *

# MongoDB connection
client = MongoClient("mongodb://admin:Netease%40164@10.246.225.48:41754/")
db = client.local
fs = gridfs.GridFS(db)

class BaseInfoDeal(object):
    """Base class for all info deal classes"""
    def __init__(self, collection_name=None):
        self.info_list = []
        self.collection_name = collection_name

    def read_info_from_collection(self, query: dict, limit=1000) -> list:
        """Read info from collection with query"""
        if not self.collection_name:
            raise ValueError("Collection name not set")
            
        try:
            print(f"Querying collection: {self.collection_name}")
            print(f"Query: {query}")
            
            collection = db[self.collection_name]
            cursor = collection.find(filter=query).limit(limit)
            results = [doc for doc in cursor]
            
            print(f"Query success, length: {len(results)}")
            self.info_list = results
            return results
            
        except ConnectionFailure as e:
            print(f"Connection failed: {e}")
            raise
        except PyMongoError as e:
            print(f"MongoDB operation failed: {e}")
            raise
        except Exception:
            traceback.print_exc()
            return []

    def read_info_by_id(self, info_id: str):
        """Find info by project ID"""
        for info in self.info_list:
            if info.get("project") == info_id:
                return info
        return None

    def save_info_to_collection(self, data: dict) -> tuple[bool, str]:
        """Save info to collection"""
        if not self.collection_name:
            raise ValueError("Collection name not set")
            
        try:
            collection = db[self.collection_name]
            collection.create_index("project", unique=True)
            result = collection.insert_one(data)
            return True, str(result.inserted_id)
            
        except DuplicateKeyError:
            project_name = data.get("project", "Unknown Project")
            msg = f'[DUPLICATE] Project "{project_name}" already exists in collection'
            print(msg)
            return False, msg
        except ConnectionFailure as e:
            print(f"Connection failed: {e}")
            raise
        except PyMongoError as e:
            print(f"MongoDB operation failed: {e}")
            raise

def check_requests_files(request_obj):
    """Check if file upload request is valid"""
    if 'file' not in request_obj.files:
        return False, jsonify({"error": "缺少文件参数"}), ""
        
    file = request_obj.files['file']
    if file.filename == '':
        return False, jsonify({"error": "空文件名"}), ""
        
    filename = secure_filename(file.filename)
    if not filename.endswith('.json'):
        return False, jsonify({"error": "仅支持JSON文件"}), ""
        
    return True, file, filename

def save_to_collection(collection_name: str, data: dict) -> tuple[bool, str]:
    """Save data to specified collection"""
    try:
        collection = db[collection_name]
        collection.create_index("project", unique=True)
        result = collection.insert_one(data)
        return True, str(result.inserted_id)
        
    except DuplicateKeyError:
        project_name = data.get("project", "Unknown Project")
        msg = f'[DUPLICATE] Project "{project_name}" already exists in collection'
        print(msg)
        return False, msg
    except ConnectionFailure as e:
        print(f"Connection failed: {e}")
        raise
    except PyMongoError as e:
        print(f"MongoDB operation failed: {e}")
        raise

def read_from_collection(collection_name: str, query: dict, limit=1000) -> list:
    """Read data from specified collection"""
    try:
        collection = db[collection_name]
        cursor = collection.find(filter=query).limit(limit)
        return [doc for doc in cursor]
        
    except ConnectionFailure as e:
        print(f"Connection failed: {e}")
        raise
    except PyMongoError as e:
        print(f"MongoDB operation failed: {e}")
        raise
    except Exception:
        traceback.print_exc()
        return []

def save_file_to_gridfs(info_id: str, info_type: str, file_obj: BufferedReader):
    """Save file to GridFS"""
    import time
    local_time = time.time()
    file_id = fs.put(
        file_obj,
        filename=str(local_time) + "BundleInfos_Normal.json",
        content_type='application/json',
        metadata={"info_id": info_id, "info_type": info_type}
    )
    print(f"File stored with ID: {file_id}")
    return file_id

def read_from_gridfs_by_info_id(info_id: str):
    """Read file from GridFS by info ID"""
    return fs.find({"metadata.info_id": info_id})

def read_from_gridfs(file_id):
    """Read file from GridFS by file ID"""
    grid_file = fs.get(file_id)
    info_id = grid_file.metadata["info_id"]
    data_bytes = grid_file.read()
    return info_id, data_bytes

def get_all_build_targets():
    """Get all build targets"""
    return [member.name for member in BuildTarget]

def get_all_build_schemas():
    """Get all build schemas"""
    schemas = [member.name for member in BuildSchema]
    print(schemas)
    return schemas