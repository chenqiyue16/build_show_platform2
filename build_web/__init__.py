
from flask import Blueprint
BuildWeb_blueprint = Blueprint('BuildWeb_blueprint', __name__)
from . import common_task
from . import bundle_info_deal
from . import views