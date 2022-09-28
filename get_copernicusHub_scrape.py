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

# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger("loggerinformation")

dir = os.path.dirname(os.path.abspath(__file__))

output_dir = dir+"/Result/Sentinel"
output_csv = dir+"/Result/CSV"

pg_con_info = {'host': config.db_host, 'dbname': config.db_dbname,
               'user': config.db_user, 'password': config.db_password, 'port': config.db_port}

sentinel_query_info = {'geojson': dir+'/korea_map.geojson', 'start_date': '20220601', 'relativeorbitnumber': [3, 10, 103, 110],
                       'end_date': '20220701', 'platformname': 'Sentinel-2', 'cloudcoverpercentage': [0, 30]}


def result_to_csv(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        print("Error : Creating directory. " + directory)


def scraping_info_download(user_id, user_pwd, query_info, save_csv):

    scraping_time = datetime.now()

    # connect to the API
    api = SentinelAPI(user_id, user_pwd, 'https://apihub.copernicus.eu/apihub')

   # search by polygon, time, and Hub query keywords
    footprint = geojson_to_wkt(read_geojson(query_info['geojson']))
    products = api.query(footprint,
                         date=(query_info['start_date'],
                               query_info['end_date']),
                         platformname=query_info['platformname'],
                         cloudcoverpercentage=(query_info['cloudcoverpercentage'][0], query_info['cloudcoverpercentage'][1]))

    # api.download_all(products, output_dir)
    # products convert to padndas dataframe
    products_df = api.to_dataframe(products)
    products_df['product_id'] = products_df.index
    products_df.index = range(1, len(products_df) + 1)

    if (save_csv):
        file_path = output_csv + '/products_downloadall/'
        result_to_csv(file_path)
        filename = file_path + scraping_time.strftime('%Y%m%d_%H%M%S') + '.csv'
        try:
            products_df.to_csv(filename, sep=',', na_rep='NaN')
        except:
            print("Error : Dataframe converting to csv. In scraping_info_download")

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
    for i, v in enumerate(query_info['relativeorbitnumber']):
        try:
            products = api.query(footprint,
                                 date=(query_info['start_date'],
                                       query_info['end_date']),
                                 platformname=query_info['platformname'],
                                 relativeorbitnumber=v,
                                 cloudcoverpercentage=(query_info['cloudcoverpercentage'][0], query_info['cloudcoverpercentage'][1]))
        except Exception as e:
            print("Error : Sentinelsat api.query, " + e.msg)

        if i == 0:
            # products convert to padndas dataframe
            products_df = api.to_dataframe(products)
            # print(products_df)
        else:
            # products convert to padndas dataframe and append
            products_df = products_df.append(
                api.to_dataframe(products), sort=False)
            # print(products_df)

    products_df['product_id'] = products_df.index
    products_df.index = range(1, len(products_df) + 1)

    if (save_csv):
        file_path = output_csv + '/products_info'
        result_to_csv(file_path)
        filename = file_path + "/products_info_" + \
            scraping_info_time.strftime('%Y%m%d_%H%M%S') + '_raw.csv'
        try:
            products_df.to_csv(filename, sep=',', na_rep='NaN')
        except Exception as e:
            print("Error : Dataframe converting to csv. In scraping_info " + e)

     # rename columns of dataframe
    products_df.rename(columns={'title': 'product_title', 'link': 'link', 'link_alternative': 'link_alternative', 'link_icon': 'link_icon',
                                'summary': 'product_sumry', 'ondemand': 'ondemand', 'datatakesensingstart': 'data_take_sensing_start', 'generationdate': 'gnr_date',
                                'beginposition': 'begin_position', 'endposition': 'end_position', 'ingestiondate': 'ingestion_date', 'orbitnumber': 'orbt_num',
                                'relativeorbitnumber': 'rel_orbt_num', 'cloudcoverpercentage': 'cloud_cover__pt', 'sensoroperationalmode': 'sensor_opr_mode',
                                'gmlfootprint': 'gml_ft_prt', 'footprint': 'ft_prt', 'level1cpdiidentifier': 'lvl1_cpdi_idntfr', 'tileid': 'tile_id',
                                'hv_order_tileid': 'hv_order_tileid', 'format': 'format', 'processingbaseline': 'prcsng_baseline', 'platformname': 'pltfom_nm',
                                'filename': 'file_nm', 'instrumentname': 'instrument_nm', 'instrumentshortname': 'instrument_shrt_nm', 'size': 'size',
                                's2datatakeid': 's2_data_take_id', 'producttype': 'product_type', 'platformidentifier': 'pltfom_idntfr',
                                'orbitdirection': 'orbt_drc', 'platformserialidentifier': 'pltfom_serial_idntfr', 'processinglevel': 'processing_level',
                                'datastripidentifier': 'datastrip_idntfr', 'granuleidentifier': 'granule_idntfr', 'identifier': 'idntfr', 'uuid': 'uuid',
                                'illuminationazimuthangle': 'illumination_az_angle', 'illuminationzenithangle': 'illumination_zenith_angle',
                                'vegetationpercentage': 'vegetation_pt', 'notvegetatedpercentage': 'notvegetated_pt', 'waterpercentage': 'water_pt',
                                'unclassifiedpercentage': 'unclassified_pt', 'mediumprobacloudspercentage': 'mediumprobaclouds_pt',
                                'highprobacloudspercentage': 'highprobaclouds_pt', 'snowicepercentage': 'snowice_pt',
                                'product_id': 'product_id'}, inplace=True)

    # selective dataframe
    # products_df_filtered = products_df[["product_id", "title", "summary", "datatakesensingstart", "orbitnumber", "relativeorbitnumber", "orbitdirection",
    # "footprint", "ingestiondate", "cloudcoverpercentage", "platformserialidentifier", "processinglevel", "producttype"]]

    # rename dataframe columns
    # products_df_filtered.columns = ["product_id", "product_title", "product_sumry", "data_take_sensing_start",  "orbt_num", "rel_orbt_num", "orbt_drc",
    # "ft_prt", "ingestion_date", "cloud_cover_percentage", "platform_serial_identifier", "processing_level", "product_type"]

    # add scraping time
    products_df['scraping_date'] = scraping_info_time

    # add status
    products_df['status'] = "offline"

    # add update_time
    products_df['update_date'] = scraping_info_time

    if (save_csv):
        file_path = output_csv + '/products_info'
        result_to_csv(file_path)
        filename = file_path + "/products_info_" + \
            scraping_info_time.strftime('%Y%m%d_%H%M%S') + '.csv'
        try:
            products_df.to_csv(filename, sep=',', na_rep='NaN')
        except:
            print("Error : Dataframe converting to csv. In scraping_info")

    result = len(products_df)
    print(f"result : {result}")

    print("scraping wss_copernicus_product_info end!")
    return products_df


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
                    execute_values(downloaded_df, con_info,
                                   'wss_copernicus_product_file')
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

    select_query = "select product_id, product_title from wss_copernicus_product_info where status != 'downloaded' order by data_take_sensing_start asc"
    retrieval_query = "select product_id from wss_copernicus_product_info where status = 'retrieval' order by data_take_sensing_start asc"
    update_query = "update wss_copernicus_product_info set status = %s, update_date = %s  where product_id = %s"

    try:
        # connect to the API
        api = SentinelAPI(
            user_id, user_pwd, 'https://apihub.copernicus.eu/apihub')

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
                cur.execute(update_query, ('downloaded',
                            status_update_time, row.product_id))
                conn.commit()
            elif not downloaded and is_online:
                print(f'Product {row.product_id} is online. update status')
                cur.execute(update_query, ('online',
                            status_update_time, row.product_id))
                conn.commit()
            else:
                try:
                    api.trigger_offline_retrieval(row.product_id)
                except(exceptions.LTAError, exceptions.ServerError) as error:
                    print("Error: %s" % error)
                    print(
                        f'Product {row.product_id} is not online. retrieval resource full')
                    cur.execute(update_query, ('offline',
                                status_update_time, row.product_id))
                    conn.commit()
                else:
                    print(
                        f'Product {row.product_id} is not online. Request retrieval')
                    cur.execute(update_query, ('retrieval',
                                status_update_time, row.product_id))
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
    query = "select * from wss_copernicus_product_info where status = 'online' order by data_take_sensing_start asc"
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

def df_to_sql(df, con_info, table):

    print("excute insert values start!")

    try:
        # create connection to postgresql
        # engine = sa.create_engine("postgresql://scott:tiger@192.168.0.199/test")
        engine = sa.create_engine(
            "postgresql://" + con_info['user'] + ":" + con_info['password'] + "@" + con_info['host'] + "/" + con_info['dbname'])
        conn = engine.connect()

        # run test
        df.to_sql(
            table, conn, index=False, if_exists="append", method="multi"
        )
        print("the dataframe is inserted")
        print("excute insert values end!")

    except (Exception, SQLAlchemyError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()
        engine.dispose()


def scraping(conf, query, con_info, csv):
    print("Sentinel Info scraping Start!")

    # scraping info
    info_df = scraping_info(conf.copernicus_id, conf.copernicus_password,
                            query, csv)

    # Select methods for insert dataframe
    # A. inser query execute by psycopg2
    # execute_values(info_df, con_info, 'wss_copernicus_product_info')

    # B. inser query execute by sqlalchemy
    df_to_sql(info_df, con_info, 'wss_copernicus_product_info')

    print("Sentinel Info scraping End!")


def download(conf, con_info, csv):
    print("Sentinel File download Start!")

    # create download target list
    list_df = create_download_list(con_info)

    # scraping satellite file by scraping_info
    scraping_download(list_df, conf.copernicus_id,
                      conf.copernicus_password, con_info, csv)

    # update scraping_info
    update_status(conf.copernicus_id,
                  conf.copernicus_password, con_info)

    print("Sentinel File download End!")

def get_start_date(con_info):
    start_date = ""
    product_id = ""
    start_date_df = pd.DataFrame(columns=["start_date"])
    
    select_query = 'select to_char(max(data_take_sensing_start), \'YYYYMMDD\') as start_date, max(product_id) as product_id from wss_copernicus_product_info'
    conn = psycopg2.connect(host=con_info['host'], dbname=con_info['dbname'],
                            user=con_info['user'], password=con_info['password'], port=con_info['port'])
    
    start_date_df = psql.read_sql(select_query, conn)
    for row in start_date_df.itertuples():
        start_date = row.start_date
        product_id = row.product_id
        
    return {
        'start_date': start_date,
        'product_id' : product_id
    }


if __name__ == "__main__":
    print("Satellite Dataset Scraper Start!")

    # start = "2015-06-01"
    # last = "2022-06-30"

    # start_date = datetime.strptime(start, "%Y-%m-%d")
    # last_date = datetime.strptime(last, "%Y-%m-%d")

    # # loop
    # while start_date <= last_date:
    #     scrap_start = start_date.strftime("%Y%m%d")
    #     print("Start Sensing date :", scrap_start)

    #     # add 1 month
    #     start_date += relativedelta(months=1)
    #     scrap_end = start_date.strftime("%Y%m%d")
    #     print("End Sensing date :", scrap_end)
        
    #     sentinel_query_info['start_date'] = scrap_start
    #     sentinel_query_info['end_date'] = scrap_end
        
    #     # Insert scraping_info top DB
    #     scraping(config, sentinel_query_info, pg_con_info, True)
    
    dateObj = get_start_date(pg_con_info)
    if dateObj is not None or dateObj['start_date'] is not None :
        sentinel_query_info['start_date'] = dateObj['start_date']
        
    print(sentinel_query_info['start_date'])
    
    sentinel_query_info['end_date'] = datetime.now().strftime('%Y%m%d');
        
    # Insert scraping_info top DB
    scraping(config, sentinel_query_info, pg_con_info, True)        

    # update scraping_info to DB
    if dateObj is None or dateObj['product_id'] is None : 
        update_status(config.copernicus_id,
                    config.copernicus_password, pg_con_info)

    # # download Satellite file to local
    # download(config, pg_con_info, True)

    # print("Satellite Dataset Scraper End!")
