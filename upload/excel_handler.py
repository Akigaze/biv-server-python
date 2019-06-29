import xlrd

from entity.Field import Field
from util import strutil
from util.DataTypeUtil import DataTypeUtil


class ExcelHandler(object):
    def __init__(self):
        self.stage_sheet = None
        self.default_sample_size = 1000

    def analyze_excel(self, stream, sheet=None):
        excel = xlrd.open_workbook(file_contents=stream.read())
        sheet = excel.sheet_by_index(0)
        sheet_name = sheet.name
        print("excel read success: %s" % sheet_name)

        row_amount, col_amount = sheet.nrows - 1, sheet.ncols
        headers = sheet.row_values(0)

        sample_size = self.default_sample_size if row_amount > self.default_sample_size else row_amount
        sample_rows = [sheet.row(i + 1) for i in range(sample_size)]

        real_header_indexes = [i for i in range(col_amount) if not strutil.empty(headers[i])]

        sheet_fields = []
        for index in real_header_indexes:
            field_name = headers[index]
            cells = [row[index] for row in sample_rows]
            ctype = DataTypeUtil.type_check(cells)
            field = Field(index, ctype, name_of_sheet=field_name, name_of_db=field_name)

            sheet_fields.append(field)

        return sheet_name, sheet_fields, row_amount

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

    def get_cell_values_of_cols(self, stream, cols):
        excel = xlrd.open_workbook(file_contents=stream.read())
        sheet = excel.sheet_by_index(0)
        row_amount = sheet.nrows
        current_row = 1
        while current_row < row_amount:
            row_values = [sheet.cell_value(current_row, col) for col in cols]
            yield row_values
            current_row += 1
        return None


