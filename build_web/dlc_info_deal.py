"""
DLC information processing.
"""
import json
from bson import json_util
from .common_task import BaseInfoDeal,save_to_collection,save_file_to_gridfs,read_from_collection
from .project_setting import DLC_COLLECTION, DLC_DESIGN_MAP_COLLECTION, METADATA_VERSION

class DLCInfoDeal(BaseInfoDeal):
    def __init__(self):
        super().__init__(DLC_COLLECTION)
        self.dlc_design_map_collect_name = DLC_DESIGN_MAP_COLLECTION

    def save_dlc_info_to_collection(self, info_id: str, build_time: str, dlcs: dict):
        """Save DLC info to collection"""
        data = {
            "project": info_id, 
            "build_time": build_time, 
            "dlcs": dlcs, 
            "metadata": {"version": METADATA_VERSION}
        }
        return self.save_info_to_collection(data)

    def save_dlc_design_map_to_collection(self, info_id: str, build_time: str, dlcs: dict):
        """Save DLC design map to collection"""
        data = {
            "project": info_id, 
            "build_time": build_time, 
            "dlcs": dlcs, 
            "metadata": {"version": METADATA_VERSION}
        }
        return save_to_collection(self.dlc_design_map_collect_name, data)

    def get_dlc_info_from_collection(self, query: dict):
        """Get DLC info from collection"""
        results = read_from_collection(
            collection_name=self.collection_name,
            query=query
        )
        print(f"Select success, length: {len(results)}")
        self.info_list = results
        
        if len(results) == 0:
            return {}
        return json.loads(json_util.dumps(results[0].get("dlcs", {})))

    def get_dlc_design_map_from_collection(self, query: dict):
        """Get DLC design map from collection"""
        results = read_from_collection(
            collection_name=self.dlc_design_map_collect_name,
            query=query
        )
        print(f"Select success, length: {len(results)}")
        
        if len(results) == 0:
            return {}
        return json.loads(json_util.dumps(results[0].get("dlcs", {})))

    def get_dlc_info_list_by_info_id(self, info_id):
        """Get DLC info list by info ID"""
        query = {
            "project": {
                "$regex": f"^{info_id}",
                "$options": "i"
            }
        }
        #print(f"Query: {query}")
        return self.get_dlc_info_from_collection(query)

    def get_dlc_design_data_map_list_by_info_id(self, info_id):
        """Get DLC design data map by info ID"""
        query = {
            "project": {
                "$regex": f"^{info_id}",
                "$options": "i"
            }
        }
        #print(f"Query: {query}")
        return self.get_dlc_design_map_from_collection(query)