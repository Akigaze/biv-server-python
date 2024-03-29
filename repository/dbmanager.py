from datetime import datetime

import pymysql
from pymysql import DataError

from config.dbconfig import CONNECTION_CONFIG
from constant.sqltemplate import show_tables_template, drop_table_template
from util.sqlutil import SQLUtil, LOG_FILE_PATH


class DataBaseManager(object):

    def __init__(self):
        self.__connection = None
        self.__cursor = None
        self.rowcount = 0
        self.error_count = 0
        self.error_stack = []

    def get_connection(self):
        if self.__connection is None:
            self.__connection = pymysql.connect(**CONNECTION_CONFIG)
        return self.__connection

    def get_cursor(self):
        if self.__cursor is None:
            self.__cursor = self.get_connection().cursor()
        return self.__cursor

    def close_connection(self):
        if self.__cursor:
            self.__cursor.close()
            self.__cursor = None
        if self.__connection:
            self.__connection.close()
            self.__connection = None
        self.rowcount = 0
        self.error_count = 0
        if len(self.error_stack) > 0:
            log_file_path = LOG_FILE_PATH % datetime.now().strftime("%Y-%m-%d %H-%M-%S")
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
            # print("insert %d " % self.rowcount)
            return
        except TypeError as err:
            error = "%s" % err
            print("TypeError: ", err, sql, args)
        except DataError as err:
            error = "%s" % err
            print("DataError: ", err, sql, args)
        except pymysql.err.InternalError as err:
            error = "%s" % err
            print("InternalError: ", err,  sql, args)

        self.error_count += 1
        self.error_stack.append((error, sql, args))

    def batch_insert(self, args):
        for sql, params in args:
            self.insert(sql, params)
        self.commit()
        print("batch insert complete: %d rows, total: %d" % (len(args), self.rowcount))

    def commit(self):
        if self.__connection:
            self.__connection.commit()


if __name__ == "__main__":
    pass
