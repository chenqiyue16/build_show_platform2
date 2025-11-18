"""
The flask application package.
"""

from flask import Flask

from .build_web import BuildWeb_blueprint

app = Flask(__name__)

app.register_blueprint(BuildWeb_blueprint, url_prefix='/BuildWeb')
