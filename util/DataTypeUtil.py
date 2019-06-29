from constant.db_field_type import DBDataTypes
from constant.special_char_of_file import NONE_VALUE
from util import strutil


class DataTypeUtil(object):
    @staticmethod
    def type_distinct(types):
        return types[0] if len(types) > 0 else None

    @staticmethod
    def type_check(cells):
        values = [cell.value for cell in cells if not strutil.equals(cell.value, NONE_VALUE)]
        if len(values) == 0:
            return DBDataTypes.varchar % DBDataTypes.DEFAULT_VARCHAR_LENGTH

        types = [type(value) for value in values]
        if str in types:
            return DBDataTypes.varchar % DBDataTypes.DEFAULT_VARCHAR_LENGTH

        return DBDataTypes.int if DataTypeUtil.all_int(*values) else DBDataTypes.float

    @staticmethod
    def all_int(*args):
        return False not in [int(value) == value for value in args]

