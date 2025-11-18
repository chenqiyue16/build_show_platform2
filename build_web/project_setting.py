"""
Routes and views for the flask application.
"""
import json
from enum import Enum

from . import BuildWeb_blueprint
from flask import Flask, render_template, request


PROJECT_CODE = "l22"
METADATA_VERSION = "0.0.1"
BUNDLEINFO_COLLECTION = "bundle_normal_infos"
SHADERVARIANT_COLLECTION = "shader_variants"
DLC_COLLECTION = "dlc_infos"
DLC_DESIGN_MAP_COLLECTION = "dlc_design_maps"
SHADER_STATS_COLLECTION = "shader_stats"



class BuildTarget(Enum):
    Android = 1
    iOS = 2
    StandaloneWindows64 = 3


class BuildSchema(Enum):
    Debug = 1,
    Release = 2,
    Publish = 3


