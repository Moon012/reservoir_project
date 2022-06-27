from database_gp import Databases
from logger_gp import*

class CRUD(Databases):
    def insertDB(self,schema,table,colum,data):
        logger = logging.getLogger()
        sql = " INSERT INTO {schema}.{table}({colum}) VALUES ('{data}) ;".format(schema=schema,table=table,colum=colum+", rgsde",data=data+"', now()")
        try:
            #print(" 인서트 쿼리" + sql)
            #logger.info(f'insert SQL  :{sql}')
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            logger.error(f'INSERT DB err  :{e}')
    
    def updateDB(self,schema,table,colum,value,condition):
        sql = " UPDATE {schema}.{table} SET {condition} WHERE {colum}='{value}' ".format(schema=schema
        , table=table , colum=colum ,value=value,condition=condition+", updde = now()" )
        
        try :
            #logger.info(f'UPDATE SQL  :{sql}')
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            logger.error(f'updaUPDATEte DB err  :{e}')
            #print(" update DB err",e)
        
    def readDB(self,schema,table,colum, condition):
        sql = " SELECT {colum} FROM {schema}.{table} WHERE {condition} ;".format(colum=colum,schema=schema,table=table,condition=condition)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            logger.info(f'SELECT SQL  :{sql}')
        except Exception as e :
            logger.error(f'SELECT DB err  :{e}')
            result = (" SELECT DB err",e)
        
        return result

    def deleteDB(self,schema,table,condition):
        sql = " DELETE from {schema}.{table} WHERE {condition} ; ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            self.db.commit()
            #logger.info(f'DELETE SQL  :{sql}')
        except Exception as e:
            logger.error(f'DELETE DB err  :{e}')
            #print( "delete DB err", e)

    def existsDB(self,schema,table,condition):
        sql = " SELECT EXISTS(SELECT * FROM {schema}.{table} WHERE {condition} ); ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            #logger.info(f'SELECT EXISTS SQL  :{sql}')
        except Exception as e:
           #logger.error(f'SELECT EXISTS DB err  :{sql}')
           result = (" SELECT EXISTS DB err",e)
        return result
     
# if __name__ == "__main__":
#     db = CRUD()
#     db.insertDB(schema='public',table='naver_news',colum='ID',data='유동적변경')
#     print(db.readDB(schema='public',table='naver_news',colum='ID'))
#     db.updateDB(schema='public',table='naver_news',colum='ID', value='와우',condition='유동적변경')
#     db.deleteDB(schema='public',table='naver_news',condition ="id != 'd'")