from constant.sql_template import create_table_template, field_define_template, single_insert_template
from entity.Field import Property


class SQLUtil(object):
    def __init__(self):
        pass

    @staticmethod
    def generate_create_scrip(table, fields):
        field_defs = [field_define_template % (field.get(Property.NAME), field.get(Property.CTYPE)) for field in fields]
        create_sql = create_table_template % (table, ",\n".join(field_defs))

        return create_sql

    @staticmethod
    def generate_insert_sql(table, field_names, values):
        fields_str = ", ".join(["`%s`" % name for name in field_names])
        values_str = ", ".join(values)
        return single_insert_template % (table, fields_str, values_str)
