from datetime import datetime

from repository.db_manager import DataBaseManager
from util.sql import SQLUtil

CREATE_TABLE_SQL_NAME_TEMPLATE = "create_table_%s_%d"
SQL_SCRIPT_PATH_TEMPLATE = "sql_script//%s.sql"


class DatabaseTableService(object):
    def __init__(self):
        self.sql_util = SQLUtil()
        self.db_manager = DataBaseManager()
        pass

    def create_table(self, name, fields, drop_existed=False):
        if drop_existed:
            self.db_manager.drop_table(name)
        result, sql = self.db_manager.create_table(name, fields)
        script_name = CREATE_TABLE_SQL_NAME_TEMPLATE % (name, datetime.now().timestamp())
        file_path = SQL_SCRIPT_PATH_TEMPLATE % script_name
        with open(file_path, "x") as stream:
            stream.write(sql)
        return True


