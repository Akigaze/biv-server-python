import xlrd


class ExcelHandler(object):
    def analysis_excel(self, file_stream):
        excel = xlrd.open_workbook(filename=None, file_contents=file_stream.read())
        sheet_names = excel.sheet_names()
        print("工作表数量: %d" % excel.nsheets)
        print("all sheets: %s" % sheet_names)

        user_sheet = excel.sheet_by_index(0)
        # excel.get_sheets()
        # excel.sheet_by_name()
        row_num, col_num = user_sheet.nrows, user_sheet.ncols
        print("sheet name: %s " % user_sheet.name)
        print("%d 行，%d 列" % (row_num, col_num))

        headers = user_sheet.row_values(0)
        return headers

