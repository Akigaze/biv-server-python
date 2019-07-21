from datetime import datetime

from config.dbconfig import BATCH_INSERT_SIZE
from entity.table import Table, Property
from repository.dbmanager import DataBaseManager
from upload.excelhandler import ExcelHandler
from util.datatypeutil import DataTypeUtil
from util.sqlutil import SQLUtil, SQL_SCRIPT_PATH_TEMPLATE

CREATE_TABLE_SQL_NAME_TEMPLATE = "create_table_%s_%d"
TABLE_NOT_EXISTED = "TABLE_NOT_EXISTED"
INSERT_SUCCESS = "INSERT_SUCCESS"


class UploadService(object):
    def __init__(self):
        self.__file_handler = ExcelHandler()
        self.sql_util = SQLUtil()
        self.db_manager = DataBaseManager()

    def analyze_excel(self, excel_stream):
        table_name, fields, count = self.__file_handler.analyze_excel(excel_stream)
        table = Table(table_name, fields, count)
        print("analyze success", table.to_json())
        return table

    def create_table(self, name, fields, drop_existed=False):
        if drop_existed:
            self.db_manager.drop_table(name)
        result, sql = self.db_manager.create_table(name, fields)
        script_name = CREATE_TABLE_SQL_NAME_TEMPLATE % (name, datetime.now().timestamp())
        file_path = SQL_SCRIPT_PATH_TEMPLATE % script_name
        with open(file_path, "x") as stream:
            stream.write(sql)
        print("save sql file : %s" % script_name)
        return True

    def insert_data(self, table, fields, excel_stream):
        if not self.db_manager.has_table(table):
            return InsertResult(TABLE_NOT_EXISTED)
        excel_col_indexes = [field.get(Property.ID) for field in fields]
        db_field_names = [field.get(Property.NAME_OF_DB) for field in fields]
        db_field_types = [field.get(Property.CTYPE) for field in fields]

        row_data = self.__file_handler.get_cell_values_of_cols(excel_stream, excel_col_indexes)
        print("data ready to insert")

        batch_data = []
        start = datetime.now()
        for row in row_data:
            placeholder_values = [
                DataTypeUtil.standard_placeholder_and_value(dtype=db_field_types[i], value=v) for i, v in enumerate(row)
            ]
            placeholder = [item[0] for item in placeholder_values]
            insert_values = [item[1] for item in placeholder_values]
            insert_sql = SQLUtil.generate_insert_sql(table, db_field_names, placeholder)
            batch_data.append((insert_sql, tuple(insert_values)))

            if len(batch_data) == BATCH_INSERT_SIZE:
                self.db_manager.batch_insert(batch_data)
                batch_data = []

        self.db_manager.batch_insert(batch_data)
        insert_count, error_count = self.db_manager.rowcount, self.db_manager.error_count
        self.db_manager.close_connection()
        cost_time = (datetime.now() - start).seconds
        print("excel data insert complete: %d rows \ncost time: %s second" % (insert_count, cost_time))
        result = InsertResult(INSERT_SUCCESS)
        result.set_detail(insert_count, error_count, cost_time)
        return result


class InsertResult(object):
    def __init__(self, result):
        self.result = result
        self.detail = None

    def set_detail(self, insert_count, error_count, cost_time, *args):
        self.detail = (insert_count, error_count, cost_time)

