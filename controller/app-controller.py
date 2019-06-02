import json

from flask import Flask, make_response
from flask import request
from flask_cors import CORS

from service.upload import UploadService
from upload.FileCache import file_cache

app = Flask(__name__)

# r'/*' 是通配符，让本服务器所有的URL 都允许跨域请求
CORS(app, resources=[r'/file', r'/table'])

upload_service = UploadService()


@app.route('/file', methods=['POST'])
def upload(*args, **keywords):
    operation = request.args.get("operation")
    excel_stream = request.files.get("file")

    table = upload_service.analysis_uploaded_file(excel_stream)

    response = make_response(table.to_json(), 200)
    return response


@app.route('/table', methods=['POST'])
def create(*args, **keywords):
    operation = request.args.get("operation")
    if operation == "CREATE":
        data_json = json.loads(request.data)
        table_name = data_json.get("tableName")
        fields = data_json.get("fields")

        result = upload_service.create_table(table_name, fields)
        file_ids = file_cache.get_file_ids()

        response_dict = {"table": table_name, "success": result, "file": file_ids}
        response = make_response(json.dumps(response_dict), 200)
        return response
    elif operation == "INSERT":
        file_stream = request.files.get("file")
        table_name = request.form.get("tableName")
        fields = json.loads(request.form.get("fields"))

        rows = upload_service.insert_multiple(table_name, fields, file_stream)
        result = {"insertRows": rows, "success": True, "table": table_name}
        response = make_response(json.dumps(result), 200)
        return response


if __name__ == '__main__':
    app.run(host="localhost", debug=True, port=9100)
