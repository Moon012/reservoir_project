import psycopg2
import database_info


class Databases():
    def __init__(self):
        self.db = psycopg2.connect(host=database_info.water_host, dbname=database_info.water_dbname,user=database_info.water_user,password=database_info.water_password,port=database_info.water_port)
        self.cursor = self.db.cursor()

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self,query,args={}):
        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.cursor.commit()