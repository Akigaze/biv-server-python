import logging
from datetime import datetime

import xlrd

from constant.db_field_type import DBDataTypes
from constant.sql_template import single_insert_template
from entity.Table import Table
from exception.ExcelReadError import ExcelReadError
from repository.db_manager import DataBaseManager
from upload.excel_handler import ExcelHandler
from util.sql import SQLUtil


class UploadService(object):
    def __init__(self):
        self.__file_handler = ExcelHandler()
        self.sql_util = SQLUtil()
        self.db_manager = DataBaseManager()
        pass

    def analyze_excel(self, excel_stream):
        try:
            excel = xlrd.open_workbook(file_contents=excel_stream.read())
        except Exception as ex:
            print("error when read excel")
            raise ExcelReadError()

        print("excel read success")
        table_name, fields, count = self.__file_handler.analyze_excel(excel)

        table = Table(table_name, fields, count)
        print("analyze success", table.to_json())
        return table

    def create_table(self, name, fields):
        sql = SQLUtil.generate_create_scrip(name, fields)
        result = self.db_manager.create_table(sql)
        script_id = "%s-%d" % (name, datetime.now().timestamp())
        file_path = "sql_script//%s.sql" % script_id
        with open(file_path, "x") as stream:
            stream.write(sql)

        return True

    def insert_multiple(self, table, fields, file_stream):
        batch_size = 10
        self.__file_handler.stage_excel_sheet(file_stream)
        col_indexes = [field.get("index") for field in fields]
        col_field_names = ["`%s`" % field.get("name") for field in fields]
        col_field_types = [field.get("type") for field in fields]

        row_data = self.__file_handler.cell_values_of_cols(col_indexes)
        self.db_manager.start_transition()
        for row in row_data:
            inserting_cols = ", ".join(col_field_names)

            insert_values = []
            insert_placeholders = []
            for i in range(len(col_field_types)):
                _type = col_field_types[i]
                _value = row[i]
                placeholder, _value = DBDataTypes.standard_placeholder_and_value(_type, _value)
                insert_placeholders.append(placeholder)
                insert_values.append(_value)

            placeholder_str = ", ".join(insert_placeholders)
            insert_sql = single_insert_template % (table, inserting_cols, placeholder_str)
            # insert_sql = insert_sql % tuple(insert_values)
            self.db_manager.insert(insert_sql, tuple(insert_values))

        insert_rows = self.db_manager.end_transition()
        return insert_rows

