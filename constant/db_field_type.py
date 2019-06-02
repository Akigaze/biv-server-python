EXCEL_CELL_TYPE = ["string", "string", "float", "float", "bool", "error", "empty"]


class DBDataTypes(object):
    varchar = "VARCHAR(%d)"
    float = "FLOAT"
    int = "INT"

    @staticmethod
    def standard_placeholder_and_value(_type, value):
        _value = value if value != "--" else None
        if _type == DBDataTypes.int:
            return "%s", int(_value)
        elif _type == DBDataTypes.float:
            return "%s", _value
        else:
            return "%s", _value
