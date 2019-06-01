from datetime import datetime

from repository.db_manager import DataBaseManager
from upload.FileCache import FileCache
from upload.excel_handler import ExcelHandler
from util.sql import SQLUtil


class UploadService(object):
    def __init__(self):
        self.__upload_handler = ExcelHandler()
        self.__file_cache = FileCache()
        self.sql_util = SQLUtil()
        self.db_manager = DataBaseManager()
        pass

    def analysis_uploaded_file(self, file_stream):
        table = self.__upload_handler.analysis_excel(file_stream)

        file_id = table.name + str(datetime.now().timestamp())
        self.__file_cache.clear_cache()
        self.__file_cache.add_file(file_id, file_stream)
        return table

    def create_table(self, name, fields):
        sql = SQLUtil.generate_create_scrip(name, fields)
        result = self.db_manager.create_table(sql)
        script_id = "%s-%d" % (name, datetime.now().timestamp())
        file_path = "sql_script//%s.sql" % script_id
        with open(file_path, "x") as stream:
            stream.write(sql)

        return True

