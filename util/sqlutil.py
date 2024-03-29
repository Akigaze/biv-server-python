import os

from constant.sqltemplate import create_table_template, field_define_template, single_insert_template
from entity.table import Property

SQL_SCRIPT_PATH_TEMPLATE = os.path.join(os.getcwd(), "sqlscript", "%s.sql")
LOG_FILE_PATH = os.path.join(os.getcwd(), "log", "db", "%s.txt")


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
