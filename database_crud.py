from database_connect import Databases

class CRUD(Databases):
    def insert_db(self,schema,table,colum,data):
        sql = " INSERT INTO {schema}.{table}({colum}) VALUES ({data}) ;".format(schema=schema,table=table,colum=colum,data=data)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" INSERT DB err",e)
    
    def update_db(self,schema,table,colum,value,condition):
        sql = " UPDATE {schema}.{table} SET {condition} WHERE {colum}='{value}' ".format(schema=schema
        , table=table , colum=colum ,value=value,condition=condition)
        
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" update DB err",e)
        
    def read_db(self,schema,table,colum, condition):
        sql = " SELECT {colum} FROM {schema}.{table} WHERE {condition} ;".format(colum=colum,schema=schema,table=table,condition=condition)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e :
            result = (" SELECT DB err",e)
        
        return result

    def delete_db(self,schema,table,condition):
        sql = " DELETE from {schema}.{table} WHERE {condition} ; ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print( "delete DB err", e)

    def exist_db(self,schema,table,condition):
        sql = " SELECT EXISTS(SELECT * FROM {schema}.{table} WHERE {condition} ); ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
        except Exception as e:
           result = (" SELECT EXISTS DB err",e)
        return result
