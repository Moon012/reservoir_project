import sys
from datetime import datetime, timedelta
from time import sleep
from xmlrpc.client import DateTime
import requests
from bs4 import BeautifulSoup
import psycopg2
import config

def reservoir_level_fomatter(s):
    if s is None:
        return None
    return format(float(s.string), ".2f")

# 저수지
reservoir = []


# DB
connection = psycopg2.connect(host=config.db_host, dbname=config.db_dbname,user=config.db_user,password=config.db_password,port=config.db_port)
cursor = connection.cursor()

# 저수지 Select
sql = "SELECT fac_code FROM wss_reservoir order by fac_code"

cursor.execute(sql)
result = cursor.fetchall()

for data in result:
    reservoir.append(data[0])

# API
reservoir_level_url = 'http://apis.data.go.kr/B552149/reserviorWaterLevel/reservoirlevel/'
reservoir_level_params = {
            'serviceKey' : config.open_api_service_key,
            'pageNo' : '1', #페이지 번호
            'numOfRows' : '365', #한 페이지 결과 수
            'fac_code' : '4423010045',  #저수지 코드
            'date_s' : '20220101',  #조회 시작 날짜
            'date_e' : '20221231'   #조회 끝 날짜, 1년이 최대
        }

# 처리 시작
print(len(reservoir))

count = 0;

for i in reservoir:
    count = count + 1
    reservoir_level_params['fac_code'] = i
    
    while True:
        try:
            start_date = int(str(datetime.today().year) + "0101")
            end_date = int((datetime.today() - timedelta(days=1)).strftime("%Y%m%d")) # 어제날짜까지 제공
            #end = int(datetime.today().strftime("%Y%m%d")) # 오늘
            
            #마지막 측정일 산출
            sql = "SELECT check_date FROM wss_water_level WHERE fac_code = '" + str(i) + "' ORDER BY check_date DESC limit 1"
            
            cursor.execute(sql)
            result = cursor.fetchone()
            
            if result is not None:
                start_date = datetime.strptime(str(result[0]), "%Y%m%d") + timedelta(days=1) # 마지막날짜에 다음날
                start_date = int(start_date.strftime("%Y%m%d"))
                
            if start_date > end_date:
                break
            
            # Insert 쿼리
            sql = "INSERT INTO wss_water_level(fac_code, check_date, rate, water_level) VALUES (%s, %s, %s, %s)"
            
            reservoir_level_params['date_s'] = str(start_date)
            reservoir_level_params['date_e'] = str(end_date)
            
            print(str(count) + " : " + reservoir_level_params['fac_code'] + " - " + reservoir_level_params['date_s'] + " ~ " + reservoir_level_params['date_e'])
            response = requests.get(reservoir_level_url, params=reservoir_level_params)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml-xml')
            
                if soup.find('returnReasonCode') is not None and soup.find('returnReasonCode').string == '00':
                    for item in soup.find_all('item'):
                        cursor.execute(sql, (item.fac_code.string, item.check_date.string, reservoir_level_fomatter(item.water_level), reservoir_level_fomatter(item.rate)))
                    
                    connection.commit()
                    break
                elif soup.find('returnReasonCode') is not None and soup.find('returnReasonCode').string == '99':
                    print("데이터 없음")
                    break
                elif soup.find('returnReasonCode') is not None and soup.find('returnReasonCode').string == '22':
                    # 서비스 요청제한 횟수 초과시 중지
                    connection.close()
                    cursor.close()
                    raise soup.find('returnAuthMsg').string
                    sys.exit()
                else:
                    # 기타 오류
                    print(soup.find('returnAuthMsg').string)
                    break
                    #raise Exception('XML PARSE ERROR')
            else:
                # Http 접속 오류
                raise Exception('HTTP CONNECTION ERROR')
        except Exception as e:
            raise e

cursor.close()
connection.close()
print("종료")