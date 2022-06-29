import psycopg2
import db_info


class Databases():
    def __init__(self):
        self.db = psycopg2.connect(host=db_info.water_host, dbname=db_info.water_dbname,user=db_info.water_user,password=db_info.water_password,port=db_info.water_port)
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