from datetime import datetime

import pymysql
from pymysql import DataError

from constant.sql_template import show_tables_template, drop_table_template
from util.sql import SQLUtil


class DataBaseManager(object):

    def __init__(self):
        self.__connection = None
        self.__cursor = None
        self.rowcount = 0
        self.error_count = 0
        self.error_stack = []
        self.connect_config = {
            "host": "localhost",
            "port": 3306,
            "user": "akigaze",
            "password": "akigaze",
            "database": "python-connect"
        }

    def get_connection(self):
        if self.__connection is None:
            self.__connection = pymysql.connect(**self.connect_config)
        return self.__connection

    def get_cursor(self):
        if self.__cursor is None:
            self.__cursor = self.get_connection().cursor()
        return self.__cursor

    def close_connection(self):
        if self.__cursor:
            self.__cursor.close()
        if self.__connection:
            self.__connection.close()
        self.rowcount = 0
        self.error_count = 0
        if len(self.error_stack) > 0:
            log_file_path = "log//%s.txt" % datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            with open(log_file_path, "x") as stream:
                content = "\n".join(["error: %s, sql: %s, args: %s" % (e, s, a) for e, s, a in self.error_stack])
                stream.write(content)
            self.error_stack = []

    def has_table(self, table):
        cursor = self.get_connection().cursor()
        cursor.execute(show_tables_template)
        tables = [t[0] for t in cursor.fetchall()]
        existed = table in tables
        print("%s%s exist in %s" % (table, "" if existed else " not", tables))
        cursor.close()
        return existed

    def drop_table(self, table):
        connection = self.get_connection()
        if self.has_table(table):
            cursor = connection.cursor()
            cursor.execute(drop_table_template % table)
            print("drop the table %s" % table)
            cursor.close()

    def create_table(self, table, fields):
        sql = SQLUtil.generate_create_scrip(table, fields)
        cursor = self.get_connection().cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        print("create table %s" % table)
        cursor.close()
        return result, sql

    def insert(self, sql, args=None):
        error = None
        try:
            cursor = self.get_cursor()
            cursor.execute(sql, args=args)
            self.rowcount += cursor.rowcount
            print("insert %d : %s" % (self.rowcount, sql))
            return
        except TypeError as err:
            error = err.__traceback__
            print("TypeError: ", err, sql, args)
        except DataError as err:
            error = err.__traceback__
            print("DataError: ", err, sql, args)
        except pymysql.err.InternalError as err:
            error = err.__traceback__
            print("InternalError: ", error, sql, args)

        self.error_count += 1
        self.error_stack.append((sql, args))

    def batch_insert(self, args):
        for sql, params in args:
            self.insert(sql, params)
        self.commit()
        print("batch insert complete: %d rows" % len(args))

    def commit(self):
        if self.__connection:
            self.__connection.commit()

if __name__ == "__main__":
    pass