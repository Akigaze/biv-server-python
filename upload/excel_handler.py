import xlrd

from constant.db_field_type import EXCEL_CELL_TYPE
from entity.Field import Field
from entity.Table import Table
from util.DataTypeUtil import DataTypeUtil


class ExcelHandler(object):
    def __init__(self):
        self.stage_sheet = None

    def analysis_excel(self, file_stream):
        excel = xlrd.open_workbook(filename=None, file_contents=file_stream.read())
        sheet = excel.sheet_by_index(0)
        table_name = sheet.name
        row_num, col_num = sheet.nrows, sheet.ncols
        headers = sheet.row_values(0)

        default_sample_size = 20
        sample_size = row_num - 1 if row_num - 1 < default_sample_size else default_sample_size

        real_header_indexes = [i for i in range(col_num) if headers[i] != ""]
        sample_rows = [sheet.row(i + 1) for i in range(sample_size)]

        table_fields = []
        for index in real_header_indexes:
            cells = [row[index] for row in sample_rows]
            col_type = DataTypeUtil.type_check(cells)
            field_name = headers[index]
            field = Field(index, field_name, field_name, col_type)
            table_fields.append(field)

        table = Table(table_name, table_fields, row_num)

        return table

    def stage_excel_sheet(self, file_stream):
        excel = xlrd.open_workbook(filename=None, file_contents=file_stream.read())
        self.stage_sheet = excel.sheet_by_index(0)

    def get_cells_value_from_stage_sheet(self, start_row, end_row, cols):
        cur_row = start_row
        result = []
        while cur_row <= end_row:
            row_values = [self.stage_sheet.cell_value(cur_row, col) for col in cols]
            result.append(row_values)
            cur_row += 1
        return result

    def cell_values_of_cols(self, cols):
        cur_row = 1
        row_num = self.stage_sheet.nrows
        while cur_row < row_num:
            row_values = [self.stage_sheet.cell_value(cur_row, col) for col in cols]
            yield row_values
            cur_row += 1
        return "end"


