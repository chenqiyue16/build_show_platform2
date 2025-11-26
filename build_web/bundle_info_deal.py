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

    def group_bundles(self, data):
        """将资源按GroupType分组"""
        grouped_data = {}

        for bundle in data["Bundles"]:
            group_type = bundle["GroupType"]

            # 初始化分组数据
            if group_type not in grouped_data:
                grouped_data[group_type] = []

            # 添加bundle到对应分组
            grouped_data[group_type].append(bundle)

        return grouped_data

    def calculate_stats_from_grouped_data(self, grouped_data):
        """通过分组数据计算统计信息"""
        print("process bundle info")

        # 初始化统计数据结构
        all_stats = defaultdict(lambda: {"count": 0, "total_size": 0})
        internal_stats = defaultdict(lambda: {"count": 0, "total_size": 0})
        suffix_stats = defaultdict(lambda: defaultdict(lambda: {"count": 0}))

        # 处理每个分组
        for group_type, bundles in grouped_data.items():
            all_asset_paths = set()
            internal_asset_paths = set()
            suffix_asset_paths = defaultdict(set)

            # 处理每个bundle
            for bundle in bundles:
                is_internal = bundle["IsInternal"]
                size = bundle["Size"]

                # 累加总大小
                all_stats[group_type]["total_size"] += size
                if is_internal:
                    internal_stats[group_type]["total_size"] += size

                # 处理每个资源
                for asset in bundle["Assets"]:
                    asset_path = asset["AssetPath"]

                    # 收集所有资源路径（去重）
                    all_asset_paths.add(asset_path)

                    # 收集内部资源路径（去重）
                    if is_internal:
                        internal_asset_paths.add(asset_path)

                    # 按后缀收集资源路径（去重）
                    suffix = os.path.splitext(asset_path)[1].lower() or "NoExtension"
                    suffix_asset_paths[suffix].add(asset_path)

            # 更新计数
            all_stats[group_type]["count"] = len(all_asset_paths)
            internal_stats[group_type]["count"] = len(internal_asset_paths)

            # 更新后缀统计
            for suffix, paths in suffix_asset_paths.items():
                suffix_stats[group_type][suffix]["count"] = len(paths)

        # 转换为列表格式
        all_stats = [{'name': group, 'count': stats['count'], 'total_size': stats['total_size']}
                     for group, stats in all_stats.items()]

        internal_stats = [{'name': group, 'count': stats['count'], 'total_size': stats['total_size']}
                          for group, stats in internal_stats.items()]

        suffix_stats = [{'name': f"{group}_{suffix}", 'count': suffix_stats[group][suffix]['count'], 'total_size': 0}
                        for group in suffix_stats
                        for suffix in suffix_stats[group]]

        return all_stats, internal_stats, suffix_stats

    def get_enhanced_group_details(self, data, group_type=None):
        """增强版分组详情获取方法"""
        # 使用您现有的group_bundles方法
        grouped_data = self.group_bundles(data)

        result = []
        for group_name, bundles in grouped_data.items():
            # 组筛选
            if group_type and group_name != group_type:
                continue

            group_info = {
                "group_name": group_name,
                "bundle_count": len(bundles),
                "total_size": sum(b.get("Size", 0) for b in bundles),
                "total_assets": sum(len(b.get("Assets", [])) for b in bundles),
                "bundles": []
            }

            # 处理每个Bundle
            for bundle in sorted(bundles, key=lambda x: x.get("Size", 0), reverse=True):
                bundle_info = {
                    "file_name": bundle.get("FileName"),
                    "size": bundle.get("Size", 0),
                    "is_internal": bundle.get("IsInternal", False),
                    "asset_count": len(bundle.get("Assets", [])),
                    "assets": []
                }

                # 处理每个Asset
                for asset in bundle.get("Assets", []):
                    bundle_info["assets"].append({
                        "path": asset.get("AssetPath"),
                        "size": asset.get("Size", 0)
                        # 添加其他需要的资产字段
                    })

                group_info["bundles"].append(bundle_info)

            result.append(group_info)

        # 按组名排序
        return sorted(result, key=lambda x: x["group_name"])

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