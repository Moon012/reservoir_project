from database_connect import Databases

class CRUD(Databases):
    def insert_db(self,table,colum,data,schema="public"):
        sql = " INSERT INTO {schema}.{table}({colum}) VALUES ({data}) ;".format(schema=schema,table=table,colum=colum,data=data)
        try:
            self.cursor.execute(sql)
            self.db.commit()
            #print('insert_db : ', data)
        except Exception as e :
            print(" INSERT DB err",e)
    
    def update_db(self,table,colum,value,condition,schema="public"):
        sql = " UPDATE {schema}.{table} SET {condition} WHERE {colum}='{value}' ".format(schema=schema
        , table=table , colum=colum ,value=value,condition=condition)
        
        try :
            self.cursor.execute(sql)
            self.db.commit()
            #print('update_db : ', condition)
        except Exception as e :
            print(" update DB err",e)
        
    def read_db(self,table,colum,condition,schema="public"):
        sql = " SELECT {colum} FROM {schema}.{table} WHERE {condition} ;".format(colum=colum,schema=schema,table=table,condition=condition)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e :
            print (" SELECT DB err", e)
        
        return result
    
    def select_one(self,table,colum,condition,schema="public"):
        sql = " SELECT {colum} FROM {schema}.{table} WHERE {condition} ;".format(colum=colum,schema=schema,table=table,condition=condition)
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchone()[0]
        
        except Exception as e :
            raise (" SELECT DB err", e)

    def delete_db(self,table,condition,schema="public"):
        sql = " DELETE from {schema}.{table} WHERE {condition} ; ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print( "delete DB err", e)

    def exist_db(self,table,condition,schema="public"):
        sql = " SELECT EXISTS(SELECT * FROM {schema}.{table} WHERE {condition} ); ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
        except Exception as e:
           result = (" SELECT EXISTS DB err",e)
        return result

    def self_db(self, self_sql):
        sql = self_sql
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e :
            result = (" SELECT DB err",e)

        return result