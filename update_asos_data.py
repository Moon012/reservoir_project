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
url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'
params = {
            'serviceKey' : 'gKwMHq7ihGLuc/D41kRJP5xjtNjcl/eQHsOhiaJTbXUpnATpQFaC+Nby8aYFv5No+Pme9T9zuhbGJbrS3zBWMA==',
            'pageNo' : '1', #페이지 번호
            'numOfRows' : '365', #한 페이지 결과 수
            'dataType' : 'XML',
            'dataCd' : 'ASOS',
            'dateCd' : 'DAY',
            'startDt' : '20100101',
            'endDt' : '20100601',
            'stnIds' : '108'
        }

# 관측소
fac = []

# DB Connection
connection = psycopg2.connect(host='192.168.123.132', dbname='water',user='postgres',password='pispdb2021',port=5432)
cursor = connection.cursor()

# 관측소 Select
sql = "SELECT station_id FROM tb_asos WHERE end_date IS NULL"

cursor.execute(sql)
result = cursor.fetchall()

for data in result:
    fac.append(data[0])

# 처리 시작
count = 0;

for i in fac:
    count = count + 1
    params['stnIds'] = i
    
    while True:
        try:
            start = int(str(datetime.today().year) + "0101")
            end = int((datetime.today() - timedelta(days=1)).strftime("%Y%m%d")) # 어제날짜까지 제공
            
            #마지막 측정일 산출
            sql = "SELECT tm FROM tb_asos_data WHERE stnid = '" + str(i) + "' ORDER BY tm DESC limit 1"
            
            cursor.execute(sql)
            result = cursor.fetchone()
            
            if result is not None:
                start = datetime.strptime(str(result[0]), "%Y-%m-%d") + timedelta(days=1) # 마지막날짜에 다음날
                start = int(start.strftime("%Y%m%d"))
                
            if start > end:
                break
            
            # Insert 쿼리
            sql = "INSERT INTO tb_asos_data(stnid, stnnm, tm, avgta, minta, mintahrmt, maxta, maxtahrmt, mi10maxrn, mi10maxrnhrmt, hr1maxrn, hr1maxrnhrmt, sumrndur, sumrn, maxinsws, maxinswswd, maxinswshrmt, maxws, maxwswd, maxwshrmt, avgws, hr24sumrws, maxwd, avgtd, minrhm, minrhmhrmt, avgrhm, avgpv, avgpa, maxps, maxpshrmt, minps, minpshrmt, avgps, ssdur, sumsshr, hr1maxicsrhrmt, hr1maxicsr, sumgsr, ddmefs, ddmefshrmt, ddmes, ddmeshrmt, sumdpthfhsc, avgtca, avglmac, avgts, mintg, avgcm5te, avgcm10te, avgcm20te, avgcm30te, avgm05te, avgm10te, avgm15te, avgm30te, avgm50te, sumlrgev, sumsmlev, n99rn, iscs, sumfogdur) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                
            params['startDt'] = str(start)
            params['endDt'] = str(end)
            
            print(str(count) + " : " + params['stnIds'] + " - " + str(start) + " ~ " + str(end))
            response = requests.get(url, params=params, allow_redirects=False)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml-xml')
                
                if soup.find('resultCode') is not None and soup.find('resultCode').string == '00':
                    for item in soup.find_all('item'):
                        cursor.execute(sql, (item.stnId.string, item.stnNm.string, item.tm.string, xstr(item.avgTa), xstr(item.minTa), xstr(item.minTaHrmt), xstr(item.maxTa), xstr(item.maxTaHrmt), xstr(item.mi10MaxRn), xstr(item.mi10MaxRnHrmt), xstr(item.hr1MaxRn), xstr(item.hr1MaxRnHrmt), xstr(item.sumRnDur), xstr(item.sumRn), xstr(item.maxInsWs), xstr(item.maxInsWsWd), xstr(item.maxInsWsHrmt), xstr(item.maxWs), xstr(item.maxWsWd), xstr(item.maxWsHrmt), xstr(item.avgWs), xstr(item.hr24SumRws), xstr(item.maxWd), xstr(item.avgTd), xstr(item.minRhm), xstr(item.minRhmHrmt), xstr(item.avgRhm), xstr(item.avgPv), xstr(item.avgPa), xstr(item.maxPs), xstr(item.maxPsHrmt), xstr(item.minPs), xstr(item.minPsHrmt), xstr(item.avgPs), xstr(item.ssDur), xstr(item.sumSsHr), xstr(item.hr1MaxIcsrHrmt), xstr(item.hr1MaxIcsr), xstr(item.sumGsr), xstr(item.ddMefs), xstr(item.ddMefsHrmt), xstr(item.ddMes), xstr(item.ddMesHrmt), xstr(item.sumDpthFhsc), xstr(item.avgTca), xstr(item.avgLmac), xstr(item.avgTs), xstr(item.minTg), xstr(item.avgCm5Te), xstr(item.avgCm10Te), xstr(item.avgCm20Te), xstr(item.avgCm30Te), xstr(item.avgM05Te), xstr(item.avgM10Te), xstr(item.avgM15Te), xstr(item.avgM30Te), xstr(item.avgM50Te), xstr(item.sumLrgEv), xstr(item.sumSmlEv), xstr(item.n99Rn), xstr(item.iscs), xstr(item.sumFogDur)))
                    connection.commit()
                    break
                elif soup.find('resultCode') is not None and soup.find('resultCode').string == '03':
                    # No Data
                    break
                elif soup.find('resultCode') is not None and soup.find('resultCode').string == '22':
                    # 서비스 요청제한 횟수 초과시 중지
                    print(soup.find('resultMsg').string)
                    connection.close()
                    cursor.close()
                    sys.exit()
                else:
                    # 기타 오류
                    raise Exception('XML PARSE ERROR')
            else:
                # Http 접속 오류
                raise Exception('HTTP CONNECTION ERROR')
        except Exception as e:
            print(e)
            print("SLEEP 10sec...")
            sleep(10)
            print("Retry")
            continue

connection.close()
cursor.close()
print("종료")