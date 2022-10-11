from tracemalloc import start
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt, exceptions
from datetime import datetime
import psycopg2
import logging
import psycopg2.extras as extras
import pandas as pd
import pandas.io.sql as psql
import os
import config
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from dateutil.relativedelta import relativedelta

#logging.basicConfig(level=logging.DEBUG)
#logger = logging.getLogger("loggerinformation")

geojsonDir = os.path.dirname(os.path.abspath(__file__))

dir = "/gp_server/copernicus_hub"
output_dir = dir+"/Result/Sentinel"
output_csv = dir+"/Result/CSV"

pg_con_info = {'host': config.db_host, 'dbname': config.db_dbname,
               'user': config.db_user, 'password': config.db_password, 'port': config.db_port}

sentinel_query_info = {'geojson': geojsonDir+'/korea_map.geojson', 'start_date': '20220601', 'relativeorbitnumber': [3, 10, 103, 110],
                       'end_date': '20220701', 'platformname': 'Sentinel-2', 'cloudcoverpercentage': [0, 30]}


def result_to_csv(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        print("Error : Creating directory. " + directory)

def scraping_download(product_df, user_id, user_pwd, con_info, save_csv):

    scraping_download_time = datetime.now()

    downloaded_df = pd.DataFrame(columns=["product_id", "file_name", "file_size", "file_path", "file_download_date"])
    try:
        # connect to the API
        api = SentinelAPI(user_id, user_pwd, 'https://apihub.copernicus.eu/apihub', show_progressbars=False)

        for row in product_df.itertuples():
            download_time = datetime.now()

            product_info = api.get_product_odata(row.product_id, full=True)
            print(f"{row.product_id} : get odata.")

            is_online = product_info['Online']

            if is_online:
                print(
                    f'Product {row.product_id} is online. Starting download.')
                try:
                    api.download(row.product_id, output_dir)
                except(exceptions.InvalidChecksumError):
                    print('Error : {row.product_id} MD5 checksum')
                except(exceptions.LTAError):
                    print('LTAError : {row.product_id} LTAError')
                except(exceptions.LTATriggered):
                    print('LTATriggered : {row.product_id} LTATriggered')
                else:
                    downloaded_df.loc[row.Index] = [product_info['id'], product_info['Filename'],
                                                    product_info['file_size'], output_dir + "/" + product_info['Filename'], download_time]
                    # downloaded file insert to db
                    execute_values(downloaded_df, con_info, 'wss_copernicus_product_file')
            else:
                print(f'Product {row.product_id} is not online.')

    except (Exception) as error:
        print("Error: %s" % error)
        return 1

    if (save_csv):
        file_path = output_csv + '/products_download'
        result_to_csv(file_path)
        filename = file_path + "/products_download_" + \
            scraping_download_time.strftime('%Y%m%d_%H%M%S') + '.csv'
        try:
            downloaded_df.to_csv(filename, sep=',', na_rep='NaN')
        except:
            print("Error : Dataframe converting to csv. In scraping_download")


def update_status(user_id, user_pwd, con_info):
    print("update wss_copernicus_product_info status start!")

    status_update_time = datetime.now()

    update_list = pd.DataFrame(columns=["product_id"])

    conn = psycopg2.connect(host=con_info['host'], dbname=con_info['dbname'],
                            user=con_info['user'], password=con_info['password'], port=con_info['port'])
    cur = conn.cursor()

    select_query = "select product_id, product_title from wss_copernicus_product_info where status != 'downloaded' order by data_take_sensing_start desc"
    retrieval_query = "select product_id from wss_copernicus_product_info where status = 'retrieval' order by data_take_sensing_start desc"
    update_query = "update wss_copernicus_product_info set status = %s, update_date = %s  where product_id = %s"

    try:
        # connect to the API
        api = SentinelAPI(user_id, user_pwd, 'https://apihub.copernicus.eu/apihub', show_progressbars=False)

        update_list = psql.read_sql(select_query, conn)
        # retrieval_list = psql.read_sql(retrieval_query, conn)
        # retrieval_list_cnt = len(retrieval_list)

        for row in update_list.itertuples():

            product_info = api.get_product_odata(row.product_id, full=True)
            print(f"{row.product_id} : get odata.")

            is_online = product_info['Online']

            filename = output_dir + "/" + row.product_title + '.zip'
            downloaded = os.path.exists(filename)

            if downloaded:
                print(f'Product {row.product_id} is downloaded. update status')
                cur.execute(update_query, ('downloaded', status_update_time, row.product_id))
                conn.commit()
            elif not downloaded and is_online:
                print(f'Product {row.product_id} is online. update status')
                cur.execute(update_query, ('online', status_update_time, row.product_id))
                conn.commit()
            else:
                try:
                    api.trigger_offline_retrieval(row.product_id)
                except(exceptions.LTAError, exceptions.ServerError) as error:
                    print("Error: %s" % error)
                    print(
                        f'Product {row.product_id} is not online. retrieval resource full')
                    cur.execute(update_query, ('offline', status_update_time, row.product_id))
                    conn.commit()
                else:
                    print(
                        f'Product {row.product_id} is not online. Request retrieval')
                    cur.execute(update_query, ('retrieval', status_update_time, row.product_id))
                    conn.commit()

        print("update wss_copernicus_product_info status end!")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1
    finally:
        cur.close()
        conn.close()


def create_download_list(con_info):

    download_list_df = pd.DataFrame(columns=["product_id"])

    conn = psycopg2.connect(host=con_info['host'], dbname=con_info['dbname'],
                            user=con_info['user'], password=con_info['password'], port=con_info['port'])
    query = "select * from wss_copernicus_product_info where status = 'online' order by data_take_sensing_start desc"
    try:
        download_list_df = psql.read_sql(query, conn)
        print("create download list")
        return download_list_df
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1
    finally:
        conn.close()

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
        print("the dataframe is inserted")
        print("excute insert values end!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    finally:
        cursor.close()
        conn.close()

def get_start_date(con_info):
    start_date = ""
    start_date_df = pd.DataFrame(columns=["start_date"])
    
    select_query = 'select to_char(max(data_take_sensing_start), \'YYYYMMDD\') as start_date from wss_copernicus_product_info'
    conn = psycopg2.connect(host=con_info['host'], dbname=con_info['dbname'],
                            user=con_info['user'], password=con_info['password'], port=con_info['port'])
    
    start_date_df = psql.read_sql(select_query, conn)
    for row in start_date_df.itertuples():
        start_date = row.start_date
        
    return start_date

def download(conf, con_info, csv):
    print("Sentinel File download Start!")

    # create download target list
    list_df = create_download_list(con_info)

    # scraping satellite file by scraping_info
    scraping_download(list_df, conf.copernicus_id, conf.copernicus_password, con_info, csv)

    # update scraping_info
    update_status(conf.copernicus_id, conf.copernicus_password, con_info)

    print("Sentinel File download End!")


if __name__ == "__main__":
    # # download Satellite file to local
    download(config, pg_con_info, True)
