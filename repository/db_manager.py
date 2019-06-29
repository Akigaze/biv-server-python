import pymysql
from pymysql import DataError


class DataBaseManager(object):
    def __init__(self):
        self.__connection = None
        self.__cursor = None
        self.__effect_rows = 0
        pass

    def __create_connection(self):
        connect_config = {
            "host": "localhost",
            "port": 3306,
            "user": "akigaze",
            "password": "akigaze",
            "database": "python-connect"
        }
        self.__connection = pymysql.connect(**connect_config)

    def create_table(self, sql):
        self.__create_connection()
        cursor = self.__connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        self.__connection.close()
        return result

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


