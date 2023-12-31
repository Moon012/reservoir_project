from os import replace
from bs4 import BeautifulSoup
from numpy import RAISE
import requests
import re
import requests
from sqlalchemy import false, null
from database_crud import CRUD  #db 연결 관련 클래스
import unicodedata
import time
from datetime import datetime, timedelta
import psycopg2
import pandas as pd

#각 크롤링 결과 저장하기 위한 리스트 선언
title_text=[]
link_text=[]
source_text=[]
date_text=[]
contents_text=[]
link_category_cd=[]  # 네이버 기사 카테고리 코드

table_name = 'wss_news_colct' #뉴스 크롤링 테이블
kwrd_table_name = 'wss_news_colct_kwrd_info' #뉴스 키워드 테이블

# 모든 신문사
#press_num_list = ['2844']

press_num_list = ['1005', '1020', '1021', '1022' , '1023', '1025' , '1028', '1032', '1081', '1469' ,
                   '2041', '2268', '2312', '2385', '2844']

## 네이버 지면
#국민일보, 동아일보, 문화일보, 세계일보, 조선일보, 중앙일보, 한겨레, 경향신문, 서울신문, 한국일보  10개
naver_press_num = ['1005', '1020', '1021', '1022' , '1023', '1025' , '1028', '1032', '1081', '1469' ]

## 네이버 지면 아닌 곳
#천지일보, 아시아투데이, 내일신문, 매일일보, 전국매일신문 5개
other_press_num = ['2041', '2268' ,'2312', '2385', '2844']

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59'}

category_dic = {'정치':'100', '경제':'101', '사회': '102', '생활/문화': '103', '세계': '104', 'IT/과학': '105', '연예': '106', '오피니언': '110'}

# DB 접속
db_connect = CRUD()

# 크롤링 하다가 갑자기 멈췄을 때
def get_requests_url(url) :
    try:
        return  requests.get(url, headers=header)
    except Exception as e:
        time.sleep(1)
        return get_requests_url(url)

#네이버 뉴스 요약정보 수집
def get_news_list(news_keyword, sort, start_date, end_date, news_keyword_cd):

    s_from = start_date.replace(".", "")
    e_to = end_date.replace(".", "")
    news_result_arr = []  #모든 기사내용 newstitles, newsurls, newsource
    link_text_all = []

    #신문사 15개 모두
    for press_num in press_num_list :
        print("신문사번호 : ", press_num)

        #모든 페이지
        for page in range(1, 200000, 10):

            title_text=[] #기사제목
            link_text=[] #기사 url
            link_category_cd=[]  # 네이버 기사 카테고리 코드
            source_text=[] #신문사
            date_text=[] #게시일
            contents_text=[] #기사내용

            #네이버 검색조건 검색 후 기사 목록
            news_list_url = "https://search.naver.com/search.naver?where=news&query=" + news_keyword + "&sort="+sort+"&ds=" + start_date + "&de=" + end_date + "&nso=so%3Ar%2Cp%3Afrom" + s_from + "to" + e_to + "&mynews=1&office_type=1&office_section_code=1&news_office_checked="+ press_num + "&start=" + str(page)

            response = get_requests_url(news_list_url)
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')

            news_no_result = soup.select('div.api_noresult_wrap > div.not_found02')

            # 없는 페이지일 경우
            if news_no_result :
                break

            if not news_no_result:

                #네이버 뉴스에서 정형화된 것들을 가져올 때
                if press_num in naver_press_num :

                    #네이버 링크주소
                    naver_links = soup.select('div.info_group > a.info')

                    for naver_link in naver_links:

                        if ( 'press' not in naver_link['class']) :

                            naver_news_link = naver_link['href']

                            #카테고리가 있는 경우만
                            if "sid=" in naver_news_link:
                                naver_news_link_arr = re.split('sid=', naver_news_link)
                                naver_news_sid = naver_news_link_arr[1]
                                link_category_cd.append(naver_news_sid)

                            rs_herf = get_requests_url(naver_news_link)
                            response_history = rs_herf.history

                            #리다이렉트 할 때 url 가져옴
                            if response_history:
                                naver_news_link = rs_herf.url

                            link_text.append(naver_news_link)
                            link_text_all.append(naver_news_link)

                    #기사 제목 추출
                    atags = soup.select('.news_tit')
                    for atag in atags:
                        title_text.append(atag.text.replace("'","''"))


                # 네이버 뉴스 지면 아닌 것
                if press_num in other_press_num :
                    #일반 주소
                    atags = soup.select('.news_tit')
                    for atag in atags:
                        title_text.append(atag.text.replace("'", "''")) #제목
                        link_text.append(atag['href'])   #링크주소
                        link_text_all.append(atag['href'])
                        link_category_cd.append('000') #카테고리 알수없음

                #신문사 추출
                source_lists = soup.select('.info_group > .press')
                for source_list in source_lists:
                    thisSource = source_list.text.strip("언론사 선정")
                    source_text.append(thisSource)

                    #검색조건에 해당하는 모든 목록
                for news_sj, news_url, new_source, news_cl_code in zip(title_text, link_text, source_text, link_category_cd):

                    news_result_arr.append([news_sj, news_url, new_source, news_cl_code, press_num])

            page += 10
            print("page : ", page)

    print("------------------------------------ 기사 목록 INSERT 시작 ------------------------------------")

    for i in enumerate(news_result_arr,  start = 1):

        news_result = i[1]

        insert_column = "'"+news_result[0] +"', '"+ news_result[1]+"', '"+ news_result[2]+"', '"+s_from+" 00:00:00', '" + news_result[3] +"', now(), '"+news_result[4]+"'"

        #뉴스 수집 키워드 정보
        insert_column2 = "'"+news_result[1] +"', 1, '"+ news_keyword_cd+"', 'admin', now()"

        #같은 url 존재 여부 확인 PK
        exist_flag = db_connect.exist_db(schema='public', table=table_name, condition ="news_url = '"+news_result[1]+"'")[0]

        #크롤링 데이터에 같은 url이 없으면
        if(exist_flag == False) :
            db_connect.insert_db(schema='public',table=table_name,colum='news_sj, news_url, news_nsprc, news_rgsde, news_cl_code, rgsde, news_nsprc_cd',data=insert_column)
            db_connect.insert_db(schema='public',table=kwrd_table_name,colum='news_url, kwrd_manage_no, kwrd_colct_code, register_id, rgsde',data=insert_column2)
            print('INSERT 기사/키워드----------- {aa}/{bb} -- start_date: {ii}, end_date: {ff}, now_url: {ss}'.format(ii=start_date, ff=end_date, ss=news_result[1], aa=i[0], bb=len(news_result_arr)))

        #크롤링 데이터에 같은 url이 있으면
        else :
            kwrd_exist_flag = db_connect.exist_db(schema='public', table=kwrd_table_name, condition ="news_url = '"+news_result[1]+"' AND kwrd_manage_no = 1 AND kwrd_colct_code = '"+news_keyword_cd+"'")[0]

            #같은 키워드가 없으면
            if(kwrd_exist_flag == False) :
                db_connect.insert_db(schema='public',table=kwrd_table_name,colum='news_url, kwrd_manage_no, kwrd_colct_code, register_id, rgsde',data=insert_column2)
                print("INSERT 키워드----------- 같은 URL 존재 : O / 뉴스 URL : ", news_result[1])

    print("------------------ 기사 목록 INSERT 완료 ------------------")
    print("------------------ 기사 목록 UPDATE 시작 ------------------")
    update_news_content(start_date,  end_date, news_keyword_cd)
    print("------------------ 기사 UPDATE 완료 ------------------")


#기간에 해당하는 목록 상세 내용 UPDATE
def update_news_content(start_date, end_date, news_keyword_cd):
    # 기간에 해당하는 크롤링 정보, 키워드 코드 join해서 가져옴
    sql = "SELECT  A.news_sn, A.news_url, B.kwrd_colct_code FROM (SELECT news_sn, news_url FROM wss_news_colct WHERE news_rgsde BETWEEN  to_timestamp('" + start_date.replace(".","-")+" 00:00:00' , 'YYYY-MM-DD HH24:MI:SS') AND  to_timestamp('"+ end_date.replace(".","-") + " 23:59:59', 'YYYY-MM-DD HH24:MI:SS')) A left join wss_news_colct_kwrd_info B ON A.news_url = B.news_url WHERE B.kwrd_colct_code = '" + news_keyword_cd + "' ORDER BY B.news_url,  B.kwrd_colct_code;"
    update_url_list = db_connect.self_db(sql)

    now_url = ''
    for i in enumerate(update_url_list,  start = 1):
        now_url = i[1][1]
        req = get_requests_url(now_url)
        req.encoding = 'UTF-8'
        soup = BeautifulSoup(req.text,'html.parser')

        comment_flag = False
        comment_cd = '0000'
        news_cl_code = None

        #내일신문 2312
        if 'naeil.com' in now_url:
            news_rgsde = soup.select('div.date')

            if news_rgsde :
                if (len(news_rgsde) == 1) :
                    comment_flag = True
                    comment_cd = '0001'

                else :
                    news_rgsde = news_rgsde[1].text.replace(" 게재","")
                    news_updde = 'null' #없음
                    news_text = soup.find('div',{'id' : 'contents'}).text
                    news_wrter = soup.find('div',{'class':'byLine'}).text.strip()

            if not news_rgsde :
                comment_flag = True
                comment_cd = '0001'

        # 매일일보 2385
        elif 'm-i.kr' in now_url:
            news_rgsde_tag = soup.select_one('div.info-text > ul > li:nth-child(2)')
            if news_rgsde_tag :
                news_rgsde = news_rgsde_tag.text.replace(" 승인 ","").replace(".","-")
            if not news_rgsde_tag :
                news_rgsde = 'null'

            news_updde = 'null'

            news_text_tag = soup.find('div', {'itemprop' : 'articleBody'})
            if news_text_tag :
                news_text = news_text_tag.text
            if not news_text_tag :
                news_text = 'null'

            news_wrter_tag = soup.select_one('div.info-text > ul > li:nth-child(1)')
            if news_wrter_tag :
                news_wrter = news_wrter_tag.text.replace(" ","").replace("'","''").strip()
            if not news_wrter_tag :
                news_wrter = ''

        # 아시아투데이 2268
        elif 'asiatoday.co' in now_url:

            news_date_tag = soup.select_one('#section_top > div > dl > dd > span')

            if (news_date_tag != None) :
                news_date_arr = news_date_tag.text.replace("기사승인 ","").replace(" ","").split(".")
                news_rgsde = news_date_arr[0] + "-" + news_date_arr[1]+"-"+news_date_arr[2]+" "+news_date_arr[3]
                news_updde = 'null'
                news_text = soup.find('div',{'class':'news_bm'}).text

                reporter_tag = soup.select_one('#section_top > div > dl > dd > div > ul > li.gija_gubun > a')
                if(reporter_tag != None) :
                    news_wrter = reporter_tag.text.strip()
                else :
                    news_wrter = ''

            else :
                comment_flag = True
                comment_cd = '0001'

        # 전국매일신문 2844
        elif 'jeonmae.co' in now_url:

            news_rgsde_tag = soup.select_one('div.info-text > ul > li:nth-child(2)')
            if news_rgsde_tag :
                news_rgsde = news_rgsde_tag.text.replace(" 승인 ","").replace(".","-")
            if not news_rgsde_tag :
                news_rgsde = 'null'

            news_updde = 'null'

            news_text_tag = soup.find('div', {'itemprop' : 'articleBody'})
            if news_text_tag :
                news_text = news_text_tag.text
            if not news_text_tag :
                news_text = 'null'

            news_wrter_tag = soup.select_one('div.info-text > ul > li:nth-child(1)')
            if news_wrter_tag :
                news_wrter = news_wrter_tag.text.strip()
            if not news_wrter_tag :
                news_wrter = ''

        # 천지일보 2041
        elif 'newscj' in now_url:

            if( soup != None and soup.text != '' ) :

                #입력, 수정 날짜 처리
                date_info_list = soup.select('#articleViewCon > article > header > ul > li');
                news_rgsde = 'null';
                news_updde = 'null';
                for a in date_info_list:
                    date_info_text = a.text
                    if '입력 ' in date_info_text:
                        news_rgsde = date_info_text.replace("입력 ","")

                    if '수정 ' in date_info_text:
                        news_updde = date_info_text.replace("수정 ","")

                news_text_tag = soup.select_one('#article-view-content-div')
                if news_text_tag :
                    news_text = soup.select_one('#article-view-content-div').text.strip()
                if not news_text_tag:
                    news_text = 'null'

                news_wrter_tag = soup.select_one('#articleViewCon > article > header > div > article.press-info');
                if news_wrter_tag :
                    news_wrter = news_wrter_tag.text.replace("기자명","")
                if not news_wrter_tag:
                    news_wrter = ''

            else :
                comment_flag = True
                comment_cd = '0001'


        # 그 외 네이버 뉴스
        elif 'naver.com' in now_url:

            # 목록 긁을 때 처리 하는데 한번 더
            if('sports' not in now_url and 'entertain' not in now_url):

                news_date_tag = soup.select('div.media_end_head_info_datestamp_bunch > span')
                if news_date_tag :
                    if (len(news_date_tag) == 1) :
                        news_rgsde = news_date_tag[0].attrs['data-date-time']
                        news_updde = 'null'
                    if (len(news_date_tag) == 2) :
                        news_rgsde = news_date_tag[0].attrs['data-date-time'] #입력일
                        news_updde = news_date_tag[1].attrs['data-modify-date-time']  #수정일

                    news_text_tag = soup.find('div', {'id' : 'dic_area'})
                    if(news_text_tag != None) :
                        news_text = news_text_tag.text
                    else :
                        news_text = ''

                    reporter_tag = soup.select_one('#contents > div.byline > p > span')
                    reporter_tag_2 = soup.select_one('#dic_area > a')

                    if(reporter_tag != None) :
                        news_wrter = reporter_tag.text.strip()

                    elif(reporter_tag == None) :
                        news_wrter = ''

                    if('sid=004' in now_url or 'sid=111' in now_url or 'sid=116' in now_url or 'sid=122' in now_url or  'sid=139' in now_url or 'sid=154' in now_url):

                        category_tag = soup.select_one('#contents > div.media_end_categorize > a > em')
                        if(category_tag != None) :
                            news_category_text = category_tag.text
                            if news_category_text not in category_dic :
                                news_cl_code = '001' #알수없음
                            else :
                                news_cl_code = category_dic[news_category_text]

                        elif(category_tag == None) :
                            news_cl_code = '999' #분류없음

                else :
                    comment_flag = True
                    comment_cd = '0001'
            else :
                #스포츠/연예 가 아닌 것만
                comment_flag = True
                comment_cd = '0002'
        else :
            comment_flag = True
            comment_cd = '0001'


        if(comment_flag == False) :

            news_text = news_text.strip().replace("\n","").replace("\r","").replace("<br>","").replace("\t","").replace("\x00","").replace("\x01","").replace("\x07","").replace("\x13","").replace("\xa0","")

            # 쿼리 홑따옴표 처리때문에 마지막 replace
            news_text_clean = news_text.replace("'","''")

            if (news_rgsde != 'null'):
                news_rgsde = "'"+news_rgsde+"'"

            if (news_updde != 'null'):
                news_updde = "'"+news_updde+"'"

            if (news_wrter != 'null' or news_wrter != ''):
                news_wrter = news_wrter.strip().replace("'","''").replace("\n","").replace("\r","").replace("<br>","").replace("\t","").replace("글 사진 ","")

            if (news_cl_code != None):
                news_condition = "news_bdt = '"+ news_text_clean + "', " +"news_rgsde = "+news_rgsde+ ", news_updde = "+ news_updde +", "  +"news_wrter = '"+news_wrter+ "' , news_dc_code = '"+comment_cd+"' , news_cl_code = '"+news_cl_code+ "', updde = now()"
            else :
                news_condition = "news_bdt = '"+ news_text_clean + "', " +"news_rgsde = "+news_rgsde+ ", news_updde = "+ news_updde +", "  +"news_wrter = '"+news_wrter+ "' , news_dc_code = '"+comment_cd+ "', updde = now()"

            db_connect.update_db(schema='public', table=table_name, colum='news_url', value=now_url, condition=news_condition)
            print('UPDATE ----------- {aa}/{bb} -- start_date: {ii}, end_date: {ff}, now_url: {ss}'.format(ii=start_date, ff=end_date, ss=now_url, aa=i[0], bb=len(update_url_list)))

        else :
            news_condition = "news_dc_code = '"+ comment_cd + "'"
            db_connect.update_db(schema='public', table=table_name, colum='news_url', value=now_url, condition=news_condition)
            print('UPDATE ----------- {aa}/{bb} -- start_date: {ii}, end_date: {ff}, now_url: {ss}'.format(ii=start_date, ff=end_date, ss=now_url, aa=i[0], bb=len(update_url_list)))


def do_crawling():
    # 1. 크롤링할 키워드 목록을 가져옴
    manage_no = db_connect.select_one(schema='public', table='wss_news_kwrd_manage', colum='kwrd_manage_no', condition ='use_yn = \'Y\' and delete_yn = \'N\'')
    news_keywords = db_connect.read_db(schema='public', table='wss_news_colct_kwrd', colum='kwrd_colct_nm, kwrd_colct_code', condition ='kwrd_manage_no=' + str(manage_no))
    

    for news_keyword in news_keywords:

        news_keyword_cd = news_keyword[1] #키워드 코드
        news_keyword = news_keyword[0] #키워드 : 가뭄, 폭염, 홍수
        sort = '1'

        # 2. 키워드 기간 설정
    
        sql = "select to_char(MAX(news_rgsde + interval '1 day'), 'YYYY.MM.DD') as last_date from wss_news_colct a, wss_news_colct_kwrd_info b where a.news_url = b.news_url and b.kwrd_colct_code = \'"+news_keyword_cd+"\';"
        last_insert_date = db_connect.self_db(sql)[0][0]
    
        # last_insert_date = db_connect.read_db(schema='public', table=table_name, colum='to_char( MAX(news_rgsde + interval \'1 day\'), \'YYYY.MM.DD\') as last_date', condition ='1=1')[0][0] #쌓여있는 디비의 가장 최근일자, 형태 : 2022.09.01
        if (last_insert_date is None or last_insert_date == '' ) : #DB 가 비워져 있을 경우 처음부터 시작
            last_insert_date = '1990.01.01'
        now_date = (datetime.now() - timedelta(days=1)).strftime("%Y.%m.%d") #오늘 - 1, 형태 2022.09.01

        start_date = last_insert_date
        end_date = now_date

        # start_date = '1990.01.01'
        # end_date = '2022.08.31'

        print('크롤링시작 ----------- 키워드: {ii}, 시작날짜: {ff}, 종료날짜: {ss}'.format(ii=news_keyword, ff=start_date, ss=end_date))

        # 3. 총기간을 년도별로 잘라서 크롤링 실행
        tm_ms = pd.period_range(start=start_date, end=end_date, freq='Y')
        year_list = list(tm_ms.astype(str))

        for this_year in year_list :

            this_year = this_year+'.01.01' #2022.01.01

            #서치한 년도랑 시작년도가 다르면
            if(this_year[:4] != start_date[:4]) :
                new_start_date = this_year
            #서치한 년도랑 시작년도가 같으면
            if(this_year[:4] == start_date[:4]) :
                new_start_date = start_date
            #서치한 년도랑 종료년도가 다르면 말일까지
            if(end_date[:4] != start_date[:4]):
                new_end_date = this_year[0:4]+".12.31"
            #서치한 년도랑 종료년도가 같으면 end_date 까지
            if(this_year[:4] == end_date[:4]):
                new_end_date = end_date

            print('년도별로 잘라서 크롤링시작 ----------- 키워드: {ii}, 시작날짜: {ff}, 종료날짜: {ss}'.format(ii=news_keyword, ff=new_start_date, ss=new_end_date))

            #1번 뉴스 목록 수집
            get_news_list(news_keyword, sort, new_start_date, new_end_date, news_keyword_cd)

            #2번 목록에 해당하는 상세내용 업데이트
            #update_news_content(new_start_date,  new_end_date, news_keyword_cd)

if __name__ == "__main__":
    do_crawling()
