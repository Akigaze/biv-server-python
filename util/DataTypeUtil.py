class DataTypeUtil(object):
    @staticmethod
    def type_distinct(types):
        return types[0] if len(types) > 0 else None
