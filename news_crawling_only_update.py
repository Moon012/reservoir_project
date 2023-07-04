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
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59'}

# DB 접속
db_connect = CRUD()

# 크롤링 하다가 갑자기 멈췄을 때
def get_requests_url(url) :
    try:
        return  requests.get(url, headers=header)
    except Exception as e:
        time.sleep(1)
        return get_requests_url(url)

#기간에 해당하는 목록 상세 내용 UPDATE
def update_news_content(start_date, end_date, news_keyword_cd):
    # 기간에 해당하는 크롤링 정보, 키워드 코드 join해서 가져옴
    sql = "SELECT  A.news_sn, A.news_url, B.kwrd_colct_code FROM (SELECT news_sn, news_url FROM wss_news_colct WHERE rgsde BETWEEN  to_timestamp('" + start_date.replace(".","-")+" 00:00:00' , 'YYYY-MM-DD HH24:MI:SS') AND  to_timestamp('"+ end_date.replace(".","-") + " 23:59:59', 'YYYY-MM-DD HH24:MI:SS')) A left join wss_news_colct_kwrd_info B ON A.news_url = B.news_url WHERE B.kwrd_colct_code = '" + news_keyword_cd + "' ORDER BY B.news_url,  B.kwrd_colct_code;"
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

        # 천지일보 2041
        if 'newscj' in now_url:

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

        #기간설정
        start_date = '2022.10.22'
        end_date = '2023.07.03'

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

            #2번 목록에 해당하는 상세내용 업데이트
            update_news_content(new_start_date,  new_end_date, news_keyword_cd)

if __name__ == "__main__":
    do_crawling()
