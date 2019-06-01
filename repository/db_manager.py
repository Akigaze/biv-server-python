import pymysql


class DataBaseManager(object):
    def __init__(self):
        self.connection = None
        pass

    def __create_connection(self):
        connect_config = {
            "host": "localhost",
            "port": 3306,
            "user": "akigaze",
            "password": "akigaze",
            "database": "python-connect"
        }
        self.connection = pymysql.connect(**connect_config)

    def create_table(self, sql):
        self.__create_connection()
        cursor = self.connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        self.connection.close()
        return result
