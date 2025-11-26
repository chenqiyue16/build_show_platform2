"""
Routes and views for the flask application.
"""
import json
from functools import lru_cache
from flask import request, jsonify, render_template, make_response
from . import BuildWeb_blueprint
from .bundle_info_deal import *
from .shader_variants_count_deal import *
from .dlc_info_deal import *
from .common_task import check_requests_files
from .project_setting import PROJECT_CODE
from .common_task import *

# 缓存加载的bundle详情数据
@lru_cache(maxsize=10)
def get_cached_bundle_detail(info_id):
    """缓存bundle详情数据，减少重复加载"""
    bundle_deal = BundleInfoDeal()
    #print(bundle_deal.load_bundle_detail_info(info_id))
    return bundle_deal.load_bundle_detail_info(info_id)

# Bundle Info Routes
@BuildWeb_blueprint.route('get_bundle_info_list')
def read_from_bundle_infos():
    """Get bundle info list"""
    print("Getting bundle info list")
    platform = request.args.get('platform', "Android")
    schema = request.args.get('schema', 'Debug')

    info_list = get_bundle_info_list(platform, schema)
    print("Info list length:", len(info_list))

    return jsonify({
        'data': info_list,
        'columns': list(info_list[0].keys()) if info_list else []
    })

@BuildWeb_blueprint.route('/')
def get_bundle_info_list_test():
    """Test route for bundle info list"""
    print("Loading bundle info list page")
    info_list = get_bundle_info_list("Android", "Debug")

    for item in info_list:
        print(item)

    return render_template('bundle_info_list.html',
                           bundle_info_list=info_list,
                           build_schemas=get_all_build_schemas(),
                           build_targets=get_all_build_targets())

@BuildWeb_blueprint.route('bundle_info_detail/<info_id>')
def get_bundle_info_detail(info_id):
    """Get bundle info detail page"""
    response = make_response(render_template('bundle_info_detail.html', info_id=info_id))
    response.cache_control.max_age = 300  # 5 minutes cache
    return response


@BuildWeb_blueprint.route('get_bundle_group_bundles_size_and_count')
def get_bundle_group_bundles_size_and_count():
    """Get bundle group statistics - 优化版本"""
    info_id = request.args.get('info_id', 'l22_Android_Debug_202505191642')

    try:
        # 使用缓存的数据
        data = get_cached_bundle_detail(info_id)
        bundle_deal = BundleInfoDeal()

        # 只获取统计信息，不获取详细信息
        # all_stats, internal_stats, suffix_stats = bundle_deal.group_process_bundles_new(data)
        all_stats, internal_stats, suffix_stats = bundle_deal.calculate_stats_from_grouped_data(bundle_deal.group_bundles(data))
        print({
            "status": "success",
            "all_stats": all_stats,
            "internal_stats": internal_stats,
            "suffix_stats": suffix_stats
        })
        return jsonify({
            "status": "success",
            "all_stats": all_stats,
            "internal_stats": internal_stats,
            "suffix_stats": suffix_stats
        })

    except Exception as e:
        print(f"Error getting bundle group statistics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@BuildWeb_blueprint.route('bundle_group_details/<info_id>')
def bundle_group_details(info_id):
    """Bundle分组详情页面"""
    response = make_response(render_template('bundle_info_group_detail.html', info_id=info_id))
    response.cache_control.max_age = 300  # 5 minutes cache
    return response

@BuildWeb_blueprint.route('get_grouped_bundle_details')
def get_enhanced_group_details():
    """获取增强版分组详情"""
    info_id = request.args.get('info_id', 'l22_Android_Debug_202505191642')
    group_type = request.args.get('group_type')

    try:
        data = get_cached_bundle_detail(info_id)
        bundle_deal = BundleInfoDeal()
        details = bundle_deal.get_enhanced_group_details(data, group_type)
        print({
            "status": "success",
            "data": details
        })
        return jsonify({
            "status": "success",
            "data": details
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"获取详情失败: {str(e)}"
        }), 500

# @BuildWeb_blueprint.route('get_distribution_data')
# def get_distribution_data():
#     """Get distribution data for charts"""
#     info_id = "l22_Android_Debug_202505191642"
#
#     # 使用缓存的数据
#     data = get_cached_bundle_detail(info_id)
#     chart_data = prepare_distribution_size_chart_data_from_data(data)
#     return jsonify(chart_data)



@BuildWeb_blueprint.route('get_bundle_assets')
def get_bundle_assets():
    """Get assets for specific bundle"""
    info_id = request.args.get('info_id', 'l22_Android_Debug_202505191642')
    bundle_name = request.args.get('bundle_name')

    try:
        # 使用缓存的数据
        data = get_cached_bundle_detail(info_id)

        # 查找指定bundle
        target_bundle = None
        for bundle in data.get("Bundles", []):
            if bundle.get("FileName") == bundle_name:
                target_bundle = bundle
                break

        if not target_bundle:
            return jsonify({
                "status": "error",
                "message": f"Bundle '{bundle_name}' not found"
            }), 404

        # 提取assets信息
        assets = []
        for asset in target_bundle.get("Assets", []):
            assets.append({
                "AssetPath": asset.get("AssetPath"),
                "Hash": asset.get("Hash"),
                "Size": asset.get("Size"),
                "Type": asset.get("Type")
            })

        return jsonify({
            "status": "success",
            "bundle_name": bundle_name,
            "assets": assets,
            "total_assets": len(assets)
        })

    except Exception as e:
        print(f"Error getting bundle assets: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# Upload Routes
@BuildWeb_blueprint.route('upload_to_bundle_info_json', methods=['POST'])
def upload_to_bundle_info_json():
    """Upload bundle info JSON"""
    platform = request.form.get('platform')
    schema = request.form.get('schema')
    build_time = request.form.get('build_time')
    info_type = "json"

    print(f"Upload bundle info: {platform}, {schema}, {build_time}")

    info_id = f"{PROJECT_CODE}_{platform}_{schema}_{build_time}"
    bundle_deal = BundleInfoDeal()
    bundle_deal.save_bundle_info_to_collection(info_id, build_time)

    success, file, filename = check_requests_files(request)
    if not success:
        return file

    bundle_deal.save_bundle_info_to_gridfs(info_id, info_type, file)

    # 清除缓存，确保下次获取最新数据
    get_cached_bundle_detail.cache_clear()

    return jsonify({
        "status": "success",
        "info_id": info_id,
        "collection": BUNDLEINFO_COLLECTION
    }), 200

@BuildWeb_blueprint.route('upload_to_shader_variants_info_json', methods=['POST'])
def upload_to_shader_variants_info_json():
    """Upload shader variants info JSON"""
    platform = request.form.get('platform')
    schema = request.form.get('schema')
    build_time = request.form.get('build_time')

    print(f"Upload shader variants: {platform}, {schema}, {build_time}")

    info_id = f"{PROJECT_CODE}_{platform}_{schema}_{build_time}"
    success, file, filename = check_requests_files(request)

    if not success:
        return file

    variants = json.load(file)
    print("Variants type:", type(variants))

    shader_deal = ShaderVariantsDeal()
    shader_deal.save_shader_variants_to_collection(info_id, build_time, variants)

    return jsonify({
        "status": "success",
        "info_id": info_id,
        "collection": SHADERVARIANT_COLLECTION
    }), 200

@BuildWeb_blueprint.route('upload_to_dlc_info_json', methods=['POST'])
def upload_to_dlc_info_json():
    """Upload DLC info JSON"""
    platform = request.form.get('platform')
    schema = request.form.get('schema')
    build_time = request.form.get('build_time')

    print(f"Upload DLC info: {platform}, {schema}, {build_time}")

    info_id = f"{PROJECT_CODE}_{platform}_{schema}_{build_time}"
    dlc_deal = DLCInfoDeal()

    success, file, filename = check_requests_files(request)
    if not success:
        return file

    dlcs = json.load(file)
    success, doc_id = dlc_deal.save_dlc_info_to_collection(info_id, build_time, dlcs)

    return jsonify({
        "status": "success" if success else "failure",
        "info_id": doc_id,
        "collection": DLC_COLLECTION
    }), 200

@BuildWeb_blueprint.route('upload_to_dlc_design_map_info_json', methods=['POST'])
def upload_to_dlc_design_map_info_json():
    """Upload DLC design map JSON"""
    platform = request.form.get('platform')
    schema = request.form.get('schema')
    build_time = request.form.get('build_time')

    print(f"Upload DLC design map: {platform}, {schema}, {build_time}")

    info_id = f"{PROJECT_CODE}_{platform}_{schema}_{build_time}"
    dlc_deal = DLCInfoDeal()

    success, file, filename = check_requests_files(request)
    if not success:
        return file

    dlcs = json.load(file)
    success, doc_id = dlc_deal.save_dlc_design_map_to_collection(info_id, build_time, dlcs)

    return jsonify({
        "status": "success" if success else "failure",
        "info_id": doc_id,
        "collection": DLC_DESIGN_MAP_COLLECTION
    }), 200

# Get Data Routes
@BuildWeb_blueprint.route('/get_shader_variants_count_info_json', methods=['GET'])
def get_shader_variants_count_info_json():
    """Get shader variants count info"""
    info_id = "l22_iOS_Debug_202506041805"
    shader_deal = ShaderVariantsDeal()
    shader_variants_count_dict = shader_deal.get_shader_variants_list_by_info_id(info_id)

    return jsonify({
        "status": "success",
        "info_id": info_id,
        "collection": SHADERVARIANT_COLLECTION,
        "shader_variants_count_dict": shader_variants_count_dict
    }), 200

@BuildWeb_blueprint.route('/get_dlc_infos_count_json', methods=['GET'])
def get_dlc_infos_count_json():
    """Get DLC infos count"""
    info_id = "l22_iOS_Debug_202506041805"
    dlc_deal = DLCInfoDeal()
    dlc_infos_count_dict = dlc_deal.get_dlc_info_list_by_info_id(info_id)

    print("DLC infos count dict:", dlc_infos_count_dict)

    return jsonify({
        "status": "success",
        "info_id": info_id,
        "collection": SHADERVARIANT_COLLECTION,
        "dlc_infos_count_dict": dlc_infos_count_dict
    }), 200

@BuildWeb_blueprint.route('/get_dlc_design_map_infos_count_json', methods=['GET'])
def get_dlc_design_map_infos_count_json():
    """Get DLC design map infos count"""
    info_id = "l22_iOS_Debug_202506041805"
    dlc_deal = DLCInfoDeal()
    dlc_design_data_map_infos_count_dict = dlc_deal.get_dlc_design_data_map_list_by_info_id(info_id)

    return jsonify({
        "status": "success",
        "info_id": info_id,
        "collection": SHADERVARIANT_COLLECTION,
        "dlc_design_data_map_infos_count_dict": dlc_design_data_map_infos_count_dict
    }), 200

# Search Route
def find_asset_generator(data, target_path):
    """Generator to find assets by path"""
    for bundle in data.get("Bundles", []):
        if bundle.get("FileName") == target_path:
            yield bundle
        else:
            if any(asset.get("AssetPath") == target_path for asset in bundle.get("Assets", [])):
                yield bundle

@BuildWeb_blueprint.route('/search_from_bundle_detail', methods=['POST'])
def search_from_bundle_detail():
    """Search from bundle detail"""
    request_data = request.get_json()

    info_id = request_data.get('info_id', '')
    path = request_data.get('path', '')

    if not path or not info_id:
        return jsonify({'data': []})

    try:
        # 使用缓存的数据
        data = get_cached_bundle_detail(info_id)
        find_res = list(find_asset_generator(data, path))
        return jsonify({'data': find_res})

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'data': [], "error": str(e)}), 500