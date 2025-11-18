"""
Shader variants information processing.
"""
from .common_task import BaseInfoDeal,read_from_collection
from .project_setting import SHADERVARIANT_COLLECTION, METADATA_VERSION

class ShaderVariantsDeal(BaseInfoDeal):
    def __init__(self):
        super().__init__(SHADERVARIANT_COLLECTION)

    def save_shader_variants_to_collection(self, info_id: str, build_time: str, variants: dict):
        """Save shader variants to collection"""
        data = {
            "project": info_id, 
            "build_time": build_time, 
            "variants": variants, 
            "metadata": {"version": METADATA_VERSION}
        }
        return self.save_info_to_collection(data)

    def get_shader_variants_from_collection(self, query: dict):
        """Get shader variants from collection"""
        results = read_from_collection(
            collection_name=self.collection_name,
            query=query
        )
        print(f"Select success, length: {len(results)}")
        self.info_list = results
        
        if len(results) == 0:
            return {}
        
        # Return the first variant data
        variants = results[0].get("variants", {})
        for key, value in variants.items():
            print(value)
            return value
            
        return {}

    def get_shader_variants_list_by_info_id(self, info_id):
        """Get shader variants list by info ID"""
        query = {
            "project": {
                "$regex": f"^{info_id}",
                "$options": "i"
            }
        }
        return self.get_shader_variants_from_collection(query)