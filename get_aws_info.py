import sys
import requests
from bs4 import BeautifulSoup
import psycopg2
import config
import math

# API
url = 'http://apis.data.go.kr/1390802/AgriWeather/getObsrSpotList'
params = {
            'serviceKey' : 'gKwMHq7ihGLuc/D41kRJP5xjtNjcl/eQHsOhiaJTbXUpnATpQFaC+Nby8aYFv5No+Pme9T9zuhbGJbrS3zBWMA==',
            'Page_Size' : '200', # 한 페이지 결과 수
            'Page_No' : 1, # 페이지 번호
        }

# DB
connection = psycopg2.connect(host=config.db_host, dbname=config.db_dbname ,user=config.db_user, password=config.db_password, port=config.db_port)
cursor = connection.cursor()

# sql = "DELETE FROM wss_aws"
# cursor.execute(sql)

sql = "INSERT INTO wss_aws(obsr_spot_code, obsr_spot_nm, do_se_code, mgc_code, clmt_zone_code, comm_mthd_code, instl_la, instl_lo, instl_al, instl_adres, obsr_begin_datetm)"
sql += " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT pk_wss_aws DO UPDATE SET obsr_spot_code = %s, obsr_spot_nm = %s, do_se_code = %s, mgc_code = %s, clmt_zone_code = %s, comm_mthd_code = %s, instl_la = %s, instl_lo = %s, instl_al = %s, instl_adres = %s, Obsr_Begin_Datetm = %s "


try:
    # 처리 시작
    page_no = params["Page_No"]
    
    while(page_no) :
        response = requests.get(url, params=params)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml-xml')
            
            if soup.find('Result_Code') is not None and soup.find('Result_Code').string == '200':
                try :
                    totalCount = int(soup.find('Total_Count').string)
                except Exception as e :
                    raise Exception ("TOTAL COUNT ERROR")
                    
                try :
                    for item in soup.find_all('item'):
                        cursor.execute(sql, (item.Obsr_Spot_Code.string, item.Obsr_Spot_Nm.string, item.Do_Se_Code.string, item.Mgc_Code.string, item.Clmt_Zone_Code.string, item.Comm_Mthd_Code.string, item.Instl_La.string, item.Instl_Lo.string, item.Instl_Al.string, item.Instl_Adres.string, item.Obsr_Begin_Datetm.string, item.Obsr_Spot_Code.string, item.Obsr_Spot_Nm.string, item.Do_Se_Code.string, item.Mgc_Code.string, item.Clmt_Zone_Code.string, item.Comm_Mthd_Code.string, item.Instl_La.string, item.Instl_Lo.string, item.Instl_Al.string, item.Instl_Adres.string, item.Obsr_Begin_Datetm.string))
                        
                    connection.commit;
                    
                    page_size = int(params["Page_Size"])
                    pageCnt = math.ceil(totalCount/page_size)
                        
                    if page_no < pageCnt : 
                        page_no = page_no + 1
                    else :
                        page_no = 0

                except Exception as e :
                    connection.rollback()
                    raise e
            else:
                try : 
                    raise Exception(soup.find('Result_Msg').string)
                except Exception as e :
                    raise e
        else:
            raise Exception('HTTP CONNECTION ERROR')
        
except Exception as e : 
    raise(e)
finally:
    if cursor is not None : 
        cursor.close()
    if connection is not None : 
        connection.close()