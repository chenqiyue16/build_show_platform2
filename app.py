from flask import Flask
from build_web import BuildWeb_blueprint
from flask_cors import CORS
app = Flask(__name__)
CORS(app, resources=r'/*')  # 允许所有路由的跨域请求
app.register_blueprint(BuildWeb_blueprint, url_prefix='/BuildWeb')

if __name__ == '__main__':
    app.run()
