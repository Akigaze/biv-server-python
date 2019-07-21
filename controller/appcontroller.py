import json
import logging

from flask import Flask, make_response, abort
from flask import request
from flask_cors import CORS

from exception.excelerror import ExcelReadError
from service.uploadservice import UploadService, INSERT_SUCCESS, TABLE_NOT_EXISTED
from util import httputil
from constant.requestparams import UploadParams, OperationType

bivapp = Flask(__name__)
logging.basicConfig()
# r'/*' 是通配符，让本服务器所有的URL 都允许跨域请求
CORS(bivapp, resources=[r'/file', r'/table'])

upload_service = UploadService()


@bivapp.route('/file', methods=['POST'])
def analyze(*args, **keywords):
    response = None
    try:
        excel_stream = httputil.retrieve_request_file(request, UploadParams.FILE_NAME)
        if excel_stream:
            print("get upload file")
            table = upload_service.analyze_excel(excel_stream)
            response = make_response(table.to_json(), 200)
        else:
            response = make_response("NO File Upload", 400)
            print("no upload file")
    except ExcelReadError as err:
        response = make_response("Excel Read Error", 400)
    return response


@bivapp.route('/table', methods=['POST'])
def upload_operate(*args, **keywords):
    response = None
    operation = request.args.get(UploadParams.OPERATION)
    if operation == OperationType.CREATE:
        data_json = json.loads(request.data)
        table_name = data_json.get(UploadParams.TABLE_NAME)
        fields = data_json.get(UploadParams.FIELDS)
        is_drop_existed_table = data_json.get(UploadParams.DROP_EXISTED)

        result = upload_service.create_table(table_name, fields, drop_existed=is_drop_existed_table)

        response_dict = {"table": table_name, "success": result}
        response = make_response(json.dumps(response_dict), 200)
        return response

    elif operation == OperationType.INSERT:
        excel_stream = httputil.retrieve_request_file(request, UploadParams.FILE_NAME)
        table_name = request.form.get(UploadParams.TABLE_NAME)
        fields = json.loads(request.form.get(UploadParams.FIELDS))

        result = upload_service.insert_data(table_name, fields, excel_stream)
        if result.result == INSERT_SUCCESS:
            success_rows, fail_rows, cost_time = result.detail
            result = {"successRows": success_rows, "failRows": fail_rows, "costTime": cost_time, "success": True, "table": table_name}
            response = make_response(json.dumps(result), 200)
        elif result.result == TABLE_NOT_EXISTED:
            abort(404)

        return response

