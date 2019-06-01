import json
from datetime import datetime

from flask import Flask, make_response
from flask import request
from flask_cors import CORS

from upload.FileCache import FileCache
from upload.excel_handler import ExcelHandler

app = Flask(__name__)

# r'/*' 是通配符，让本服务器所有的URL 都允许跨域请求
CORS(app, resources=r'/*')

upload_handler = ExcelHandler()
file_cache = FileCache()

@app.route('/file', methods=['POST'])
def upload(*args, **keywords):
    operation = request.args.get("operation")
    excel_stream = request.files.get("file")
    table = upload_handler.analysis_excel(excel_stream)
    file_id = table.name + str(datetime.now().timestamp())
    file_cache.add_file(file_id, excel_stream)
    response = make_response(table.to_json(), 200)
    return response


if __name__ == '__main__':
    app.run(host="localhost", debug=True, port=9100)
