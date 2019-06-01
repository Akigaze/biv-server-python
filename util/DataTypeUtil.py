class DataTypeUtil(object):
    @staticmethod
    def type_distinct(types):
        return types[0] if len(types) > 0 else None

    @staticmethod
    def type_check(cells):
        values = [cell.value for cell in cells if cell.value != "--"]
        if len(values) == 0:
            return "VARCHAR(50)"

        types = [type(value) for value in values]
        if str in types:
            lengths = [len(str(value)) for value in values]
            return "VARCHAR(%d)" % max(lengths)

        all_int = [int(value) == value for value in values]
        if False in all_int:
            return "FLOAT"
        return "INT"
