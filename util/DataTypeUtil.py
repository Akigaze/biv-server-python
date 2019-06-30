import decimal

from constant.db_field_type import DBDataTypes
from constant.special_char_of_file import NONE_VALUE
from util import strutil

ctx = decimal.Context()
ctx.prec = 20

class DataTypeUtil(object):

    @staticmethod
    def standard_placeholder_and_value(dtype=None, value=None):
        _value = value if value != NONE_VALUE else None
        if _value:
            if dtype == DBDataTypes.int:
                return "%s", int(_value)
            elif dtype == DBDataTypes.float:
                return "%s", DataTypeUtil.float_to_str(_value)
        return "%s", _value

    @staticmethod
    def type_check(cells):
        values = [cell.value for cell in cells if not strutil.equals(cell.value, NONE_VALUE)]
        if len(values) == 0:
            return DBDataTypes.text

        types = [type(value) for value in values]
        if str in types:
            if max([len(str(value)) for value in values]) > DBDataTypes.DEFAULT_VARCHAR_LENGTH:
                return DBDataTypes.text
            return DBDataTypes.varchar % DBDataTypes.DEFAULT_VARCHAR_LENGTH
        if DataTypeUtil.all_int(*values):
            return DBDataTypes.int
        # accuracies = [DataTypeUtil.float_to_str(float(value)).split(".") for value in values if value != 0]
        # print(accuracies)
        # significant_digit = max([len(accuracy[0]) for accuracy in accuracies])
        # decimal_places = max([len(accuracy[1]) for accuracy in accuracies])
        return DBDataTypes.float

    @staticmethod
    def all_int(*args):
        return False not in [int(value) == value for value in args]

    @staticmethod
    def float_to_str(f):
        d1 = ctx.create_decimal(repr(f))
        return format(d1, 'f')



