import sys
from datetime import datetime, timedelta
from time import sleep
from xmlrpc.client import DateTime
import requests
from bs4 import BeautifulSoup
import psycopg2
import config

def asos_data_fomatter(s):
    if s is None:
        return None
    return s.string

# API
asos_data_url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'
asos_data_params = {
            'serviceKey' : config.open_api_service_key,
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
observatory = []

# DB Connection
connection = psycopg2.connect(host=config.db_host, dbname=config.db_dbname,user=config.db_user,password=config.db_password,port=config.db_port)
cursor = connection.cursor()

# 관측소 Select
sql = "SELECT station_id FROM wss_asos WHERE end_date IS NULL"

cursor.execute(sql)
result = cursor.fetchall()

for data in result:
    observatory.append(data[0])

# 처리 시작
count = 0;

for i in observatory:
    count = count + 1
    asos_data_params['stnIds'] = i
    
    while True:
        try:
            start = int(str(datetime.today().year) + "0101")
            end = int((datetime.today() - timedelta(days=1)).strftime("%Y%m%d")) # 어제날짜까지 제공
            
            #마지막 측정일 산출
            sql = "SELECT tm FROM wss_asos_data WHERE stnid = '" + str(i) + "' ORDER BY tm DESC limit 1"
            
            cursor.execute(sql)
            result = cursor.fetchone()
            
            if result is not None:
                start = datetime.strptime(str(result[0]), "%Y-%m-%d") + timedelta(days=1) # 마지막날짜에 다음날
                start = int(start.strftime("%Y%m%d"))
                
            if start > end:
                break
            
            # Insert 쿼리
            sql = "INSERT INTO wss_asos_data(stnid, stnnm, tm, avgta, minta, mintahrmt, maxta, maxtahrmt, mi10maxrn, mi10maxrnhrmt, hr1maxrn, hr1maxrnhrmt, sumrndur, sumrn, maxinsws, maxinswswd, maxinswshrmt, maxws, maxwswd, maxwshrmt, avgws, hr24sumrws, maxwd, avgtd, minrhm, minrhmhrmt, avgrhm, avgpv, avgpa, maxps, maxpshrmt, minps, minpshrmt, avgps, ssdur, sumsshr, hr1maxicsrhrmt, hr1maxicsr, sumgsr, ddmefs, ddmefshrmt, ddmes, ddmeshrmt, sumdpthfhsc, avgtca, avglmac, avgts, mintg, avgcm5te, avgcm10te, avgcm20te, avgcm30te, avgm05te, avgm10te, avgm15te, avgm30te, avgm50te, sumlrgev, sumsmlev, n99rn, iscs, sumfogdur) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                
            asos_data_params['startDt'] = str(start)
            asos_data_params['endDt'] = str(end)
            
            print(str(count) + " : " + asos_data_params['stnIds'] + " - " + str(start) + " ~ " + str(end))
            response = requests.get(asos_data_url, params=asos_data_params, allow_redirects=False)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml-xml')
                
                if soup.find('resultCode') is not None and soup.find('resultCode').string == '00':
                    for item in soup.find_all('item'):
                        cursor.execute(sql, (item.stnId.string, item.stnNm.string, item.tm.string, asos_data_fomatter(item.avgTa), asos_data_fomatter(item.minTa), asos_data_fomatter(item.minTaHrmt), asos_data_fomatter(item.maxTa), asos_data_fomatter(item.maxTaHrmt), asos_data_fomatter(item.mi10MaxRn), asos_data_fomatter(item.mi10MaxRnHrmt), asos_data_fomatter(item.hr1MaxRn), asos_data_fomatter(item.hr1MaxRnHrmt), asos_data_fomatter(item.sumRnDur), asos_data_fomatter(item.sumRn), asos_data_fomatter(item.maxInsWs), asos_data_fomatter(item.maxInsWsWd), asos_data_fomatter(item.maxInsWsHrmt), asos_data_fomatter(item.maxWs), asos_data_fomatter(item.maxWsWd), asos_data_fomatter(item.maxWsHrmt), asos_data_fomatter(item.avgWs), asos_data_fomatter(item.hr24SumRws), asos_data_fomatter(item.maxWd), asos_data_fomatter(item.avgTd), asos_data_fomatter(item.minRhm), asos_data_fomatter(item.minRhmHrmt), asos_data_fomatter(item.avgRhm), asos_data_fomatter(item.avgPv), asos_data_fomatter(item.avgPa), asos_data_fomatter(item.maxPs), asos_data_fomatter(item.maxPsHrmt), asos_data_fomatter(item.minPs), asos_data_fomatter(item.minPsHrmt), asos_data_fomatter(item.avgPs), asos_data_fomatter(item.ssDur), asos_data_fomatter(item.sumSsHr), asos_data_fomatter(item.hr1MaxIcsrHrmt), asos_data_fomatter(item.hr1MaxIcsr), asos_data_fomatter(item.sumGsr), asos_data_fomatter(item.ddMefs), asos_data_fomatter(item.ddMefsHrmt), asos_data_fomatter(item.ddMes), asos_data_fomatter(item.ddMesHrmt), asos_data_fomatter(item.sumDpthFhsc), asos_data_fomatter(item.avgTca), asos_data_fomatter(item.avgLmac), asos_data_fomatter(item.avgTs), asos_data_fomatter(item.minTg), asos_data_fomatter(item.avgCm5Te), asos_data_fomatter(item.avgCm10Te), asos_data_fomatter(item.avgCm20Te), asos_data_fomatter(item.avgCm30Te), asos_data_fomatter(item.avgM05Te), asos_data_fomatter(item.avgM10Te), asos_data_fomatter(item.avgM15Te), asos_data_fomatter(item.avgM30Te), asos_data_fomatter(item.avgM50Te), asos_data_fomatter(item.sumLrgEv), asos_data_fomatter(item.sumSmlEv), asos_data_fomatter(item.n99Rn), asos_data_fomatter(item.iscs), asos_data_fomatter(item.sumFogDur)))
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

cursor.close()
connection.close()
print("종료")