from constant.sql_template import create_table_template


class SQLUtil(object):
    def __init__(self):
        pass

    @staticmethod
    def generate_create_scrip(table, fields):
        field_defs = ["`%s` %s" % (field.get("name"), field.get("type")) for field in fields]
        create_sql = create_table_template % (table, ", ".join(field_defs))

        return create_sql
