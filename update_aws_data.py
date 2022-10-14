import sys
from datetime import datetime, timedelta
from time import sleep
from xmlrpc.client import DateTime
import requests
from bs4 import BeautifulSoup
import psycopg2
import config
import math

def xstr(s):
    if s is None:
        return None
    return s.string

# API
url = 'http://apis.data.go.kr/1390802/AgriWeather/WeatherObsrInfo/InsttWeather/getWeatherTermDayList'
params = {
            'serviceKey' : 'gKwMHq7ihGLuc/D41kRJP5xjtNjcl/eQHsOhiaJTbXUpnATpQFaC+Nby8aYFv5No+Pme9T9zuhbGJbrS3zBWMA==',
            'Page_No' : '1', #페이지 번호
            'Page_Size' : '100', #한 페이지 결과 수
            'begin_Date' : '2022-01-01',
            'end_Date' : '',
            'obsr_Spot_Code' : ''
        }

# 관측소
fac = []

# DB Connection
connection = psycopg2.connect(host=config.db_host, dbname=config.db_dbname,user=config.db_user,password=config.db_password,port=config.db_port)
cursor = connection.cursor()

# 관측소 Select
sql = "SELECT obsr_spot_code FROM wss_aws"

cursor.execute(sql)
result = cursor.fetchall()

for data in result:
    fac.append(data[0])

# 처리 시작
count = 0;

for i in fac:
    count = count + 1
    params['obsr_Spot_Code'] = i
    
    try : 
        start = str(datetime.today().year) + "-01-01"
        end = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d") # 어제날짜까지 제공
        
        #마지막 측정일 산출
        sql = "SELECT date_time FROM wss_aws_data WHERE obsr_spot_code = '" + str(i) + "' ORDER BY date_time DESC limit 1"
        
        cursor.execute(sql)
        result = cursor.fetchone()
        
        if result is not None:
            start = datetime.strptime(str(result[0]).strip(), "%Y-%m-%d") + timedelta(days=1) # 마지막날짜에 다음날
            start = start.strftime("%Y-%m-%d")
        if start > end:
            continue
        
        # Insert 쿼리
        sql = "INSERT INTO wss_aws_data(obsr_Spot_Code, obsr_Spot_Nm, date_Time, tmprt_150, tmprt_150Top, tmprt_150Lwet, tmprt_50, tmprt_50Top, tmprt_50Lwet, tmprt_400, tmprt_400Top, tmprt_400Lwet, hd_150, hd_50, hd_400, wd_300, wd_150, wd_1000, arvlty_300, arvlty_150, arvlty_1000, afp, afv, sunshn_Time, solrad_Qy, dwcn_Time, pnwg_Tp, frfr_Tp, udgr_Heatt_Cndctvt, udgr_Tp_10, udgr_Tp_5, udgr_Tp_20, soil_Mitr_10, soil_Mitr_10Cmst, soil_Mitr_20, soil_Mitr_20Cmst, soil_Mitr_30, soil_Mitr_30Cmst) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT pk_wss_aws_data DO NOTHING "
            
        params['begin_Date'] = str(start)
        params['end_Date'] = str(end)
        
        page_no = 1
        
        while page_no <= 4:
            try:
                params['Page_No'] = page_no
                
                print(str(count) + " : " + params['obsr_Spot_Code'] + " - " + str(start) + " ~ " + str(end) + " - " + str(page_no))
                
                response = requests.get(url, params=params, allow_redirects=False)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'lxml-xml')
                    
                    if soup.find('result_Code') is not None and soup.find('result_Code').string == '200':
                        try :
                            totalCount = int(soup.find('total_Count').string)
                        except Exception as e :
                            print (e)
                        
                        if totalCount:
                            pass
                        else: 
                            break
                        
                        for item in soup.find_all('item'):
                            try : 
                                if xstr(item.obsr_Spot_Code) is not None:
                                    cursor.execute(sql, (xstr(item.obsr_Spot_Code), xstr(item.obsr_Spot_Nm), xstr(item.date_Time), xstr(item.tmprt_150), xstr(item.tmprt_150Top), xstr(item.tmprt_150Lwet), xstr(item.tmprt_50), xstr(item.tmprt_50Top), xstr(item.tmprt_50Lwet), xstr(item.tmprt_400), xstr(item.tmprt_400Top), xstr(item.tmprt_400Lwet), xstr(item.hd_150), xstr(item.hd_50), xstr(item.hd_400), xstr(item.wd_300), xstr(item.wd_150), xstr(item.wd_1000), xstr(item.arvlty_300), xstr(item.arvlty_150), xstr(item.arvlty_1000), xstr(item.afp), xstr(item.afv), xstr(item.sunshn_Time), xstr(item.solrad_Qy), xstr(item.dwcn_Time), xstr(item.pnwg_Tp), xstr(item.frfr_Tp), xstr(item.udgr_Heatt_Cndctvt), xstr(item.udgr_Tp_10), xstr(item.udgr_Tp_5), xstr(item.udgr_Tp_20), xstr(item.soil_Mitr_10), xstr(item.soil_Mitr_10Cmst), xstr(item.soil_Mitr_20), xstr(item.soil_Mitr_20Cmst), xstr(item.soil_Mitr_30), xstr(item.soil_Mitr_30Cmst)))
                            except Exception as e:
                                continue    
                            
                        page_size = int(params["Page_Size"])
                        pageCnt = math.ceil(totalCount/page_size)
                        
                        if page_no < pageCnt : 
                            page_no = page_no + 1
                        else :
                            page_no = 0
                            
                    elif soup.find('Result_Code').string == '201':
                        # 데이터가 특정일자부터 없는 관측소.... API 가 참 이상하다...
                        print("No Data")
                        break
                    else:
                        # 기타 오류
                        raise Exception('XML PARSE ERROR')
                else:
                    # Http 접속 오류
                    raise Exception('HTTP CONNECTION ERROR')
                
            except Exception as e:
                raise e
        
        connection.commit()
    except Exception as e:
        cursor.close()
        connection.close()                        
        raise e

cursor.close()
connection.close()
print("종료")