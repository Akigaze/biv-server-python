import pymysql
from pymysql import DataError

from constant.sql_template import show_tables_template, drop_table_template
from util.sql import SQLUtil


class DataBaseManager(object):
    def __init__(self):
        self.__connection = None
        self.__cursor = None
        self.__effect_rows = 0
        pass

    def get_connection(self):
        if self.__connection is None:
            self.__connection = self.__create_connection()
        return self.__connection

    def close_connection(self):
        self.__connection.close()

    def __create_connection(self):
        connect_config = {
            "host": "localhost",
            "port": 3306,
            "user": "akigaze",
            "password": "akigaze",
            "database": "python-connect"
        }
        return pymysql.connect(**connect_config)

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
        try:
            self.__cursor.execute(sql, args=args)
            self.__effect_rows += self.__cursor.rowcount
            self.__connection.commit()
        except TypeError as err:
            print("exception: ", err, sql)
        except DataError as err:
            print("exception: ", err, sql)

    def batch_insert(self, args):
        for sql, params in args:
            self.insert(sql, params)
        self.__connection.commit()

    def commit(self):
        self.__connection.commit()

    def end_transition(self):
        self.__connection.commit()
        self.__cursor.close()
        self.__connection.close()
        return self.__effect_rows

    def start_transition(self):
        self.__effect_rows = 0
        self.__create_connection()
        self.__cursor = self.__connection.cursor()


