from constant.special_char_of_file import NONE_VALUE

EXCEL_CELL_TYPE = ["string", "string", "float", "float", "bool", "error", "empty"]


class DBDataTypes(object):
    varchar = "VARCHAR(%d)"
    float = "FLOAT"
    int = "INT"
    text = "TEXT"
    DEFAULT_VARCHAR_LENGTH = 255

    @staticmethod
    def standard_placeholder_and_value(dtype=None, value=None):
        _value = value if value != NONE_VALUE else None
        if _value:
            if dtype == DBDataTypes.int:
                return "%s", int(_value)
            elif dtype == DBDataTypes.float:
                return "%s", float(_value)
        return "%s", _value
