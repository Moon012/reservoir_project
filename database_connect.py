import psycopg2
import WaterDatabase


class Databases():
    def __init__(self):
        self.db = psycopg2.connect(host=WaterDatabase.water_host, dbname=WaterDatabase.water_dbname,user=WaterDatabase.water_user,password=WaterDatabase.water_password,port=WaterDatabase.water_port)
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