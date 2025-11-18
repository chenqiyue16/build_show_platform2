"""
Bundle information processing.
"""
import json
import os
import time
from collections import defaultdict
import bisect
import pandas as pd
from bson import json_util
from .common_task import BaseInfoDeal, save_file_to_gridfs, read_from_gridfs_by_info_id
from .project_setting import BUNDLEINFO_COLLECTION, METADATA_VERSION

class BundleInfoDeal(BaseInfoDeal):
    def __init__(self):
        super().__init__(BUNDLEINFO_COLLECTION)

    def save_bundle_info_to_collection(self, info_id: str, build_time: str):
        """Save bundle info to collection"""
        data = {
            "project": info_id, 
            "build_time": build_time, 
            "metadata": {"version": METADATA_VERSION}
        }
        return self.save_info_to_collection(data)

    def save_bundle_info_to_gridfs(self, info_id: str, info_type: str, file_obj):
        """Save bundle info to GridFS"""
        gridfs_id = save_file_to_gridfs(info_id, info_type, file_obj)
        print(f"Save to gridfs success, ID: {gridfs_id}")
        return gridfs_id

    def load_bundle_detail_info(self, info_id: str):
        """Load bundle detail info from GridFS"""
        grid_files = read_from_gridfs_by_info_id(info_id)
        grid_file = grid_files[0] if grid_files else None
        if grid_file:
            return json.loads(grid_file.read().decode('utf-8-sig'))
        return None

    def group_process_bundles(self, data):
        # 初始化统计数据结构
        print("process bundle info")
        all_stats = defaultdict(lambda: {"count": 0, "total_size": 0, "paths": list()})
        internal_stats = defaultdict(lambda: {"count": 0, "total_size": 0, "paths": list()})
        suffix_stats = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total_size": 0, "paths": list()}))
        # 处理每个Bundle
        for bundle in data["Bundles"]:
            group_type = bundle["GroupType"]
            is_internal = bundle.get("IsInternal", False)
            size = bundle["Size"]
            all_stats[group_type]["total_size"] += size
            if is_internal:
                internal_stats[group_type]["total_size"] += size

            # 处理每个资源
            for asset in bundle["Assets"]:
                asset_path = asset["AssetPath"]
                # 1. 全部资源统计 (确保唯一)
                if asset_path not in all_stats[group_type]["paths"]:
                    all_stats[group_type]["count"] += 1
                    #all_stats[group_type]["paths"].add(asset_path)

                # 2. 仅统计IsInternal资源 (确保唯一)
                if is_internal:
                    if asset_path not in internal_stats[group_type]["paths"]:
                        internal_stats[group_type]["count"] += 1
                        #internal_stats[group_type]["paths"].add(asset_path)

                # 3. 后缀类型统计 (确保唯一)
                suffix = os.path.splitext(asset_path)[1].lower() or "NoExtension"
                if asset_path not in suffix_stats[group_type][suffix]["paths"]:
                    suffix_stats[group_type][suffix]["count"] += 1
                    #suffix_stats[group_type][suffix]["paths"].add(asset_path)
        all_stats = [{'name': group, 'count': all_stats[group]['count'], 'total_size': all_stats[group]['total_size']} for group in list(all_stats.keys())]
        internal_stats = [{'name': group, 'count': internal_stats[group]['count'], 'total_size': internal_stats[group]['total_size']} for group in list(internal_stats.keys())]
        suffix_stats = [{'name': group, 'count': suffix_stats[group]['count'], 'total_size': suffix_stats[group]['total_size']} for group in list(suffix_stats.keys())]
        return all_stats, internal_stats, suffix_stats

    def group_process_bundles_with_details(self, data):
        """分组处理bundles，同时返回统计信息和详细信息"""
        print("process bundle info with details")

        # 初始化统计数据结构
        all_stats = defaultdict(lambda: {"count": 0, "total_size": 0, "paths": list()})
        internal_stats = defaultdict(lambda: {"count": 0, "total_size": 0, "paths": list()})
        suffix_stats = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total_size": 0, "paths": list()}))

        # 存储所有bundle的详细信息
        bundle_details = {
            "all_bundles": [],
            "internal_bundles": {"Internal": [], "NonInternal": []},
            "suffix_bundles": defaultdict(list)
        }

        # 处理每个Bundle
        for bundle in data["Bundles"]:
            group_type = bundle["GroupType"]
            is_internal = bundle.get("IsInternal", False)
            size = bundle["Size"]
            bundle_name = bundle["FileName"]

            # 存储bundle基本信息
            bundle_info = {
                "FileName": bundle_name,
                "Size": size,
                "AssetCount": len(bundle["Assets"]),
                "GroupType": group_type,
                "IsInternal": is_internal
            }

            # 添加到所有bundles
            bundle_details["all_bundles"].append(bundle_info)

            # 添加到内部/非内部分组
            if is_internal:
                bundle_details["internal_bundles"]["Internal"].append(bundle_info)
            else:
                bundle_details["internal_bundles"]["NonInternal"].append(bundle_info)

            # 统计信息
            all_stats[group_type]["total_size"] += size
            if is_internal:
                internal_stats[group_type]["total_size"] += size

            # 处理每个资源
            for asset in bundle["Assets"]:
                asset_path = asset["AssetPath"]

                # 1. 全部资源统计 (确保唯一)
                if asset_path not in all_stats[group_type]["paths"]:
                    all_stats[group_type]["count"] += 1
                    all_stats[group_type]["paths"].append(asset_path)

                # 2. 仅统计IsInternal资源 (确保唯一)
                if is_internal:
                    if asset_path not in internal_stats[group_type]["paths"]:
                        internal_stats[group_type]["count"] += 1
                        internal_stats[group_type]["paths"].append(asset_path)

                # 3. 后缀类型统计 (确保唯一)
                suffix = os.path.splitext(asset_path)[1].lower() or "NoExtension"
                if asset_path not in suffix_stats[group_type][suffix]["paths"]:
                    suffix_stats[group_type][suffix]["count"] += 1
                    suffix_stats[group_type][suffix]["paths"].append(asset_path)

                # 为后缀分组添加bundle信息
                if bundle_info not in bundle_details["suffix_bundles"][suffix]:
                    bundle_details["suffix_bundles"][suffix].append(bundle_info)

        # 转换统计结果为列表格式
        all_stats_list = [
            {'name': group, 'count': all_stats[group]['count'], 'total_size': all_stats[group]['total_size']} for group
            in list(all_stats.keys())]
        internal_stats_list = [
            {'name': group, 'count': internal_stats[group]['count'], 'total_size': internal_stats[group]['total_size']}
            for group in list(internal_stats.keys())]
        suffix_stats_list = []

        for group_type, suffixes in suffix_stats.items():
            for suffix, stats in suffixes.items():
                suffix_stats_list.append({
                    'name': f"{group_type}_{suffix}",
                    'count': stats['count'],
                    'total_size': stats['total_size'],
                    'group_type': group_type,
                    'suffix': suffix
                })

        return all_stats_list, internal_stats_list, suffix_stats_list, bundle_details

    def get_bundles_by_group(self, data, group_type, group_name):
        """根据分组类型和名称获取对应的bundles"""
        # 如果已经有缓存的分组详情，可以直接使用
        if hasattr(self, '_cached_bundle_details'):
            bundle_details = self._cached_bundle_details
        else:
            # 否则重新计算分组
            _, _, _, bundle_details = self.group_process_bundles_with_details(data)
            self._cached_bundle_details = bundle_details

        if group_type == 'all':
            return bundle_details["all_bundles"]
        elif group_type == 'internal':
            return bundle_details["internal_bundles"].get(group_name, [])
        elif group_type == 'suffix':
            return bundle_details["suffix_bundles"].get(group_name, [])
        else:
            return []

    def group_process_bundles_new(self, data):
        """分组处理bundles - 只返回统计信息，性能优化版本"""
        print("process bundle info - optimized")

        # 使用集合而不是列表来存储路径，提高查找性能
        all_stats = defaultdict(lambda: {"count": 0, "total_size": 0, "paths": set()})
        internal_stats = defaultdict(lambda: {"count": 0, "total_size": 0, "paths": set()})
        suffix_stats = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total_size": 0, "paths": set()}))

        # 处理每个Bundle
        for bundle in data["Bundles"]:
            group_type = bundle["GroupType"]
            is_internal = bundle.get("IsInternal", False)
            size = bundle["Size"]

            # 统计信息
            all_stats[group_type]["total_size"] += size
            if is_internal:
                internal_stats[group_type]["total_size"] += size

            # 处理每个资源
            for asset in bundle["Assets"]:
                asset_path = asset["AssetPath"]

                # 1. 全部资源统计 (使用集合确保唯一)
                if asset_path not in all_stats[group_type]["paths"]:
                    all_stats[group_type]["count"] += 1
                    all_stats[group_type]["paths"].add(asset_path)

                # 2. 仅统计IsInternal资源 (使用集合确保唯一)
                if is_internal:
                    if asset_path not in internal_stats[group_type]["paths"]:
                        internal_stats[group_type]["count"] += 1
                        internal_stats[group_type]["paths"].add(asset_path)

                # 3. 后缀类型统计 (使用集合确保唯一)
                suffix = os.path.splitext(asset_path)[1].lower() or "NoExtension"
                if asset_path not in suffix_stats[group_type][suffix]["paths"]:
                    suffix_stats[group_type][suffix]["count"] += 1
                    suffix_stats[group_type][suffix]["paths"].add(asset_path)

        # 转换统计结果为列表格式
        all_stats_list = [
            {'name': group, 'count': all_stats[group]['count'], 'total_size': all_stats[group]['total_size']}
            for group in list(all_stats.keys())
        ]
        internal_stats_list = [
            {'name': group, 'count': internal_stats[group]['count'], 'total_size': internal_stats[group]['total_size']}
            for group in list(internal_stats.keys())
        ]
        suffix_stats_list = []

        for group_type, suffixes in suffix_stats.items():
            for suffix, stats in suffixes.items():
                suffix_stats_list.append({
                    'name': f"{group_type}_{suffix}",
                    'count': stats['count'],
                    'total_size': stats['total_size'],
                    'group_type': group_type,
                    'suffix': suffix
                })

        return all_stats_list, internal_stats_list, suffix_stats_list

    def get_bundle_group_details(self, data, group_type, group_name):
        """获取特定分组的bundle详情 - 按需加载，提高性能"""
        bundles = data.get("Bundles", [])
        result = []

        if group_type == 'all':
            # 返回所有bundles的基本信息
            for bundle in bundles:
                result.append({
                    "FileName": bundle.get("FileName"),
                    "Size": bundle.get("Size"),
                    "AssetCount": len(bundle.get("Assets", [])),
                    "GroupType": bundle.get("GroupType"),
                    "IsInternal": bundle.get("IsInternal", False)
                })

        elif group_type == 'internal':
            # 内部资源分组
            is_internal_flag = (group_name == "Internal")
            for bundle in bundles:
                is_internal = bundle.get("IsInternal", False)
                if is_internal == is_internal_flag:
                    result.append({
                        "FileName": bundle.get("FileName"),
                        "Size": bundle.get("Size"),
                        "AssetCount": len(bundle.get("Assets", [])),
                        "GroupType": bundle.get("GroupType"),
                        "IsInternal": is_internal
                    })

        elif group_type == 'suffix':
            # 后缀名分组
            for bundle in bundles:
                assets = bundle.get("Assets", [])
                if not assets:
                    continue

                # 计算该bundle中主要后缀
                suffix_count = {}
                for asset in assets:
                    asset_path = asset.get("AssetPath", "")
                    suffix = os.path.splitext(asset_path)[1].lower() or "NoExtension"
                    suffix_count[suffix] = suffix_count.get(suffix, 0) + 1

                if suffix_count:
                    main_suffix = max(suffix_count, key=suffix_count.get)
                    if main_suffix == group_name:
                        result.append({
                            "FileName": bundle.get("FileName"),
                            "Size": bundle.get("Size"),
                            "AssetCount": len(assets),
                            "GroupType": bundle.get("GroupType"),
                            "IsInternal": bundle.get("IsInternal", False),
                            "MainSuffix": main_suffix,
                            "SuffixDistribution": suffix_count
                        })

        return result
    # def groups_distribution_size_data(self, data):
    #     """Calculate size distribution for bundles"""
    #     BOUNDARIES = [1000, 2000, 5000, 10000, 20000]
    #     LABELS = [f"{a}-{b}" for a, b in zip([0] + BOUNDARIES, BOUNDARIES + [float('inf')])]
    #     LABELS[-1] = f"{BOUNDARIES[-1]}+"
    #
    #     distribution = defaultdict(lambda: defaultdict(int))
    #     bundles = data.get("Bundles", [])
    #
    #     for bundle in bundles:
    #         group = bundle.get("GroupType", "Unknown")
    #         size = bundle.get("Size", 0)
    #         idx = bisect.bisect_right(BOUNDARIES, size)
    #         distribution[group][LABELS[idx]] += 1
    #
    #     return distribution, LABELS

def get_bundle_info_list(platform, schema):
    """Get bundle info list by platform and schema"""
    bundle_deal = BundleInfoDeal()
    query = {
        "project": {
            "$regex": f"^l22_{platform}_{schema}_",
            "$options": "i"
        }
    }
    bundle_deal.read_info_from_collection(query)
    return json.loads(json_util.dumps(bundle_deal.info_list))

# def prepare_distribution_size_chart_data(info_id):
#     """Prepare chart data for size distribution"""
#     print("Time1: " + time.ctime())
#
#     bundle_deal = BundleInfoDeal()
#     data = bundle_deal.load_bundle_detail_info(info_id)
#     print("Time2: " + time.ctime())
#
#     distribution_data, labels = bundle_deal.groups_distribution_size_data(data)
#     print("Time3: " + time.ctime())
#
#     # Convert to DataFrame for easier processing
#     df = pd.DataFrame.from_dict(
#         {k: dict(v) for k, v in distribution_data.items()},
#         orient='index'
#     ).fillna(0).reindex(columns=labels)
#
#     # Build Chart.js data structure
#     chart_data = {
#         "labels": df.index.tolist(),
#         "datasets": []
#     }
#
#     # Color scheme
#     colors = [
#         "#a6cee3", "#1f78b4", "#b2df8a", "#33a02c",
#         "#fb9a99", "#e31a1c", "#fdbf6f", "#ff7f00",
#         "#cab2d6", "#6a3d9a", "#ffff99", "#b15928"
#     ]
#
#     # Create datasets for each size range
#     for idx, size_range in enumerate(labels):
#         dataset = {
#             "label": size_range,
#             "data": df[size_range].tolist(),
#             "backgroundColor": colors[idx % len(colors)],
#             "borderColor": "white",
#             "borderWidth": 1
#         }
#         chart_data["datasets"].append(dataset)
#
#     print("Time4: " + time.ctime())
#     return json.dumps(chart_data)