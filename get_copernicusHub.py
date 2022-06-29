from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt, exceptions
from datetime import datetime, date
import psycopg2
import logging
from psycopg2.extras import LoggingConnection
import psycopg2.extras as extras
import pandas as pd
import pandas.io.sql as psql
import os.path
from os import path
import database_info

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("loggerinformation")

output_dir = "Result\Sentinel"


def scraping_info_download(user_id, user_pwd, query_info, save_csv):

    scraping_time = datetime.now()

    # connect to the API
    api = SentinelAPI(user_id, user_pwd, 'https://apihub.copernicus.eu/apihub')

   # search by polygon, time, and Hub query keywords
    footprint = geojson_to_wkt(read_geojson(query_info['geojson']))
    products = api.query(footprint,
                         date=(query_info['start_date'], query_info['end_date']),
                         platformname=query_info['platformname'],
                         cloudcoverpercentage=(query_info['cloudcoverpercentage'][0], query_info['cloudcoverpercentage'][1]))

    # api.download_all(products, output_dir)
    # products convert to padndas dataframe
    products_df = api.to_dataframe(products)
    products_df['product_id'] = products_df.index
    products_df.index = range(1, len(products_df) + 1)

    if (save_csv):
        file_name = 'products_downloadall' + \
            scraping_time.strftime('%Y%m%d_%H%M%S') + '.csv'
        products_df.to_csv(file_name, sep=',', na_rep='NaN')

    # add scraping time to dataframe
    products_df['scraping_date'] = scraping_time

    return products_df


def scraping_info(user_id, user_pwd, query_info, save_csv):
    print("scraping wss_copernicus_product_info start!")

    scraping_info_time = datetime.now()

    # connect to the API
    api = SentinelAPI(user_id, user_pwd, 'https://apihub.copernicus.eu/apihub')

    # search by polygon, time, and Hub query keywords
    footprint = geojson_to_wkt(read_geojson(query_info['geojson']))
    products = api.query(footprint,
                         date=(query_info['start_date'], query_info['end_date']),
                         platformname=query_info['platformname'],
                         cloudcoverpercentage=(query_info['cloudcoverpercentage'][0], query_info['cloudcoverpercentage'][1]))

    # products convert to padndas dataframe
    products_df = api.to_dataframe(products)
    products_df['product_id'] = products_df.index
    products_df.index = range(1, len(products_df) + 1)

    # selective dataframe
    products_df_filtered = products_df[["product_id", "product_title", "product_sumry", "datatakesensingstart",
                                        "ingestiondate", "cloudcoverpercentage", "platformserialidentifier", "processinglevel", "producttype"]]

    # rename dataframe columns
    products_df_filtered.columns = ["product_id", "product_title", "product_sumry", "data_take_sensing_start", "ingestion_date",
                                    "cloud_cover_percentage", "platform_serial_identifier", "processing_level", "product_type"]

    # add scraping time
    products_df_filtered['scraping_date'] = scraping_info_time
    
    # add status 
    products_df_filtered['status'] = "offline"
    
    # add update_time 
    products_df_filtered['update_date'] = scraping_info_time

    if (save_csv):
        file_name = 'Result\CSV\products_info_' + \
            scraping_info_time.strftime('%Y%m%d_%H%M%S') + '.csv'
        products_df_filtered.to_csv(file_name, sep=',', na_rep='NaN')
    
    result = len(products_df)
    print(f"result : {result}")
        
    print("scraping wss_copernicus_product_info end!")
    return products_df_filtered


def scraping_download(product_df, user_id, user_pwd, con_info, save_csv):

    scraping_download_time = datetime.now()

    downloaded_df = pd.DataFrame(
        columns=["product_id", "file_name", "file_size", "file_path", "file_download_date"])
    try:
        # connect to the API
        api = SentinelAPI(
            user_id, user_pwd, 'https://apihub.copernicus.eu/apihub')

        for row in product_df.itertuples():
            download_time = datetime.now()

            product_info = api.get_product_odata(row.product_id, full=True)
            print(f"{row.product_id} : get odata.")

            is_online = product_info['Online']

            if is_online:
                print(f'Product {row.product_id} is online. Starting download.')
                try:
                    api.download(row.product_id, output_dir)
                except(exceptions.InvalidChecksumError ) :
                    print('Error : {row.product_id} MD5 checksum')                    
                except(exceptions.LTAError) :
                    print('LTAError : {row.product_id} LTAError')
                except(exceptions.LTATriggered ) :
                    print('LTATriggered : {row.product_id} LTATriggered')                                        
                else:
                    downloaded_df.loc[row.Index] = [product_info['id'], product_info['Filename'],
                                                product_info['file_size'], output_dir + "\\" + product_info['Filename'], download_time]
                    # downloaded file insert to db
                    execute_values(downloaded_df, con_info, 'wss_copernicus_product_file')
            else:
                print(f'Product {row.product_id} is not online.')

    except (Exception) as error:
        print("Error: %s" % error)
        return 1

    if (save_csv):
        file_name =  'Result\CSV\products_download_' + \
            scraping_download_time.strftime('%Y%m%d_%H%M%S') + '.csv'
        downloaded_df.to_csv(file_name, sep=',', na_rep='NaN')


def update_status(user_id, user_pwd, con_info):
    print("update wss_copernicus_product_info status start!")
     
    status_update_time = datetime.now()
    
    update_list = pd.DataFrame(columns=["product_id"])

    conn = psycopg2.connect(host=con_info['host'], dbname=con_info['dbname'],
                            user=con_info['user'], password=con_info['password'], port=con_info['port'])
    cur = conn.cursor()
    
    select_query = "select product_id, product_title from wss_copernicus_product_info where status != 'downloaded' order by data_take_sensing_start asc"
    retrieval_query = "select product_id from wss_copernicus_product_info where status = 'retrieval' order by data_take_sensing_start asc"
    update_query = "update wss_copernicus_product_info set status = %s, update_date = %s  where product_id = %s" 
       
    try:
        # connect to the API
        api = SentinelAPI(
            user_id, user_pwd, 'https://apihub.copernicus.eu/apihub')
        
        update_list = psql.read_sql(select_query, conn)
        retrieval_list = psql.read_sql(retrieval_query, conn)
        retrieval_list_cnt = len(retrieval_list)
        
        for row in update_list.itertuples():

            product_info = api.get_product_odata(row.product_id, full=True)
            print(f"{row.product_id} : get odata.")

            is_online = product_info['Online']
            
            filename = output_dir + "\\" + row.product_title + '.zip'
            downloaded = path.exists(filename)
            
            if downloaded:
                print(f'Product {row.product_id} is downloaded. update status')
                cur.execute(update_query, ('downloaded', status_update_time, row.product_id))
                conn.commit()
            elif not downloaded and is_online:
                print(f'Product {row.product_id} is online. update status')
                cur.execute(update_query, ('online', status_update_time, row.product_id))
                conn.commit()
            else :
                try:
                    api.trigger_offline_retrieval(row.product_id)
                except(exceptions.LTAError, exceptions.ServerError) as error:
                    print("Error: %s" % error)
                    print(f'Product {row.product_id} is not online. retrieval resource full')
                    cur.execute(update_query, ('offline', status_update_time, row.product_id))
                    conn.commit()
                else:
                    print(f'Product {row.product_id} is not online. Request retrieval')
                    cur.execute(update_query, ('retrieval', status_update_time, row.product_id))
                    conn.commit()            
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1
        
    print("update wss_copernicus_product_info status end!")
    conn.close()
   

def create_download_list(con_info):

    download_list_df = pd.DataFrame(columns=["product_id"])

    conn = psycopg2.connect(host=con_info['host'], dbname=con_info['dbname'],
                            user=con_info['user'], password=con_info['password'], port=con_info['port'])
    query = "select * from wss_copernicus_product_info where status = 'online' order by data_take_sensing_start asc"
    try:
        download_list_df = psql.read_sql(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1
    print("create download list")
    conn.close()

    return download_list_df


def execute_values(df, con_info, table):

    print("excute insert values start!")

    # create connection to postgresql
    conn = psycopg2.connect(host=con_info['host'], dbname=con_info['dbname'],
                            user=con_info['user'], password=con_info['password'], port=con_info['port'])

    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (product_id) DO NOTHING" % (
        table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
    conn.close()
            
    print("excute insert values end!")


if __name__ == "__main__":
    print("Satellite Dataset Scraper Start!")

    pg_con_info = {'host': database_info.water_host, 'dbname': database_info.water_dbname,
                   'user': database_info.water_user, 'password': database_info.water_password, 'port': database_info.water_port}
    
    sentinel_query_info = {'geojson': 'korea_map.geojson', 'start_date': '20151225',
                           'end_date': '20160101', 'platformname': 'Sentinel-2', 'cloudcoverpercentage': [0, 30]}

    # create download target list
    list_df = create_download_list(pg_con_info)
    # scraping satellite file by scraping_info
    scraping_download(list_df, "ymseo", "sseo4655", pg_con_info, True)
    # update scraping_info
    update_status("ymseo", "sseo4655", pg_con_info)

    print("Satellite Dataset Scraper End!")
