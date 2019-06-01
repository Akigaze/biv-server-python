import xlrd

from constant.db_field_type import EXCEL_CELL_TYPE
from entity.Field import Field
from entity.Table import Table
from util.DataTypeUtil import DataTypeUtil


class ExcelHandler(object):
    def analysis_excel(self, file_stream):
        excel = xlrd.open_workbook(filename=None, file_contents=file_stream.read())
        sheet = excel.sheet_by_index(0)
        table_name = sheet.name
        row_num, col_num = sheet.nrows, sheet.ncols
        headers = sheet.row_values(0)

        default_sample_size = 5
        sample_size = row_num - 1 if row_num - 1 < default_sample_size else default_sample_size

        real_header_indexes = [i for i in range(col_num) if headers[i] != ""]
        sample_rows = [sheet.row(i + 1) for i in range(sample_size)]

        table_fields = []
        for index in real_header_indexes:
            types = [row[index].ctype for row in sample_rows]
            col_type = DataTypeUtil.type_distinct(types)
            field_name = headers[index]
            field = Field(field_name, field_name, EXCEL_CELL_TYPE[col_type])
            table_fields.append(field)

        table = Table(table_name, table_fields, row_num)

        return table

