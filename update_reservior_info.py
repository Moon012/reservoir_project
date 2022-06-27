# OPEN API 로 저수지 정보는 코드만 제공되어 개발중단

import sys
from datetime import datetime, timedelta
from time import sleep
from xmlrpc.client import DateTime
import requests
from bs4 import BeautifulSoup
import psycopg2

def xstr(s):
    if s is None:
        return None
    return s.string

# API
# API
url = 'http://apis.data.go.kr/B552149/reserviorWaterLevel/reservoircode/'
params = {
            'serviceKey' : 'gKwMHq7ihGLuc/D41kRJP5xjtNjcl/eQHsOhiaJTbXUpnATpQFaC+Nby8aYFv5No+Pme9T9zuhbGJbrS3zBWMA==',
            'pageNo' : '1', #페이지 번호
            'numOfRows' : '9999', #한 페이지 결과 수
            'county' : '',  #저수지 위치
        }

# 17개 시도
fac = ['강원도','경기도','경상남도','경상북도','광주광역시','대구광역시','대전광역시','부산광역시','서울특별시','세종특별자치시','울산광역시','인천광역시','전라남도','전라북도','제주특별자치도','충청남도','충청북도']

# DB Connection
connection = psycopg2.connect(host='192.168.123.132', dbname='water',user='postgres',password='pispdb2021',port=5432)
cursor = connection.cursor()
# Insert 쿼리
sql = "INSERT INTO tb_asos_data(fac_code, name, location, start_year, const_year, class, division, watershed_area, flood_area, full_area, benefit_area, drought_freq, flood_freq, dam_type, dam_volume, dam_width, dam_height, total_storage, effect_storage, dead_storage, intake_type, flood_level, full_level, dead_level) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

# 처리 시작
count = 0;

for i in fac:
    count = count + 1
    params['county'] = i
    
    while True:
        try:
            print(str(count) + " : " + params['county'])
            response = requests.get(url, params=params, allow_redirects=False)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml-xml')
            
                if soup.find('returnReasonCode') is not None and soup.find('returnReasonCode').string == '00':
                    for item in soup.find_all('item'):
                         total = 0;
                    break
                elif soup.find('returnReasonCode') is not None and soup.find('returnReasonCode').string == '99':
                    break
                elif soup.find('returnReasonCode') is not None and soup.find('returnReasonCode').string == '22':
                    # 서비스 요청제한 횟수 초과시 중지
                    print(soup.find('returnAuthMsg').string)
                    sys.exit()
                else:
                    # 기타 오류
                    raise Exception(soup.find('returnAuthMsg').string)
            else:
                # Http 접속 오류
                raise Exception('HTTP CONNECTION ERROR')
        except Exception as e:
            print(e)
            print("SLEEP 10sec...")
            sleep(10)
            print("Retry")
            continue

print("종료")
print(str(total))