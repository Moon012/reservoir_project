import psycopg2
import config

class Databases():
    def __init__(self):
        self.db = psycopg2.connect(host=config.db_host, dbname=config.db_dbname,user=config.db_user,password=config.db_password,port=config.db_port)
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