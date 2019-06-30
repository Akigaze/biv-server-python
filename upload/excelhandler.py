import xlrd

from entity.table import Field
from util import strutil
from util.datatypeutil import DataTypeUtil


class ExcelHandler(object):
    def __init__(self, sample_size=-1):
        self.default_sample_size = sample_size

    def cal_sample_size(self, count):
        if self.default_sample_size <= 0 or count < self.default_sample_size:
            return count
        return self.default_sample_size

    def analyze_excel(self, stream, sheet=None):
        sheet = self.get_sheet_from_stream(stream, index=0)
        sheet_name, row_amount, col_amount = sheet.name, sheet.nrows - 1, sheet.ncols
        print("excel read success: %s" % sheet_name)

        headers = sheet.row_values(0)
        real_header_indexes = [i for i in range(col_amount) if not strutil.empty(headers[i])]

        sample_size = self.cal_sample_size(row_amount)
        sample_rows = [sheet.row(i + 1) for i in range(sample_size)]

        sheet_fields = []
        for index in real_header_indexes:
            field_name = headers[index]
            cells = [row[index] for row in sample_rows]
            ctype = DataTypeUtil.type_check(cells)
            field = Field(index, ctype, name_of_sheet=field_name, name_of_db=field_name)

            sheet_fields.append(field)

        return sheet_name, sheet_fields, row_amount

    def get_cell_values_of_cols(self, stream, cols):
        sheet = self.get_sheet_from_stream(stream, index=0)
        row_amount = sheet.nrows
        current_row = 1
        while current_row < row_amount:
            row_values = [sheet.cell_value(current_row, col) for col in cols]
            yield row_values
            current_row += 1
        return None

    def get_sheet_from_stream(self, stream, index=0, name=None):
        excel = xlrd.open_workbook(file_contents=stream.read())
        if name:
            return excel.sheet_by_name(name)
        return excel.sheet_by_index(index)

