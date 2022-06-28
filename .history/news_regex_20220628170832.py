import chardet
from imp import reload
import sys
import os
import re
from konlpy.tag import Mecab, Kkma, Okt, Hannanum, Komoran
from sqlalchemy.orm import scoped_session, sessionmaker
from vo.NewsKwrdCntVo import NewsKwrdCntVo
from vo.NewsNounsExtrcVo import NewsNounsExtrcVo
from vo.NewsColctVo import NewsColctVo
from vo.CodeDtstmnVo import code_dtstmn_vo
from vo.NewsKwrdYearCntVo import NewsKwrdYearCntVo
import sqlalchemy as db
import logging as logging
import json
import collections as ct
import time

logger = logging.getLogger()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(messages)s")

# log를 console에 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# 설정 정보
with open('./config/jdbc.json') as f1, open('./config/config.json') as f2:
    jdbc_conf = json.load(f1)
    conf = json.load(f2)
    url = jdbc_conf['url']
    engine = db.create_engine(**jdbc_conf)
    limit = conf["limit"]
    page = conf["page"]
    user_id = conf["user_id"]

# mecab = Mecab("C:\\mecab\\mecab-ko-dic")
# kkma = Kkma()
# ha = Hannanum()
ko = Komoran()
# okt = Okt()

# session 획득
def get_session():
    try:
        Session = scoped_session(sessionmaker(autocommit=False, autoflush=True, expire_on_commit=False, bind=engine))
        session = Session()
        return session
    except Exception as e: 
        logger.error(e)

# session 종료
def close_session(session):
    try:
        session.close()
    except Exception as e: 
        logger.error(e)
    finally:
        if session is not None:
            session.close()
            
# 형태소 분석기를 통한 명사를 json 채로 저장
def insert_db_nouns(obj, session):
    try :
        news_id = obj["news_sn"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        nouns_obj = obj["news_nouns_cnt_obj"]
        str_nouns_obj = json.dumps(nouns_obj, ensure_ascii=False)
        valuses = []
        
        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_sn'] = news_id
        t_obj['news_noun'] = str_nouns_obj
        
        session.query(NewsColctVo).filter(NewsColctVo.news_sn == t_obj['news_sn']).update(t_obj)
        
        for key, value in nouns_obj.items():
            t_nouns_obj = dict()
            t_nouns_obj['news_sn'] = news_id
            t_nouns_obj['register_id'] = register_id
            t_nouns_obj['updusr_id'] = updusr_id
            t_nouns_obj['news_nouns'] = key
            t_nouns_obj['news_nouns_co'] = value
            valuses.append(t_nouns_obj)

        session.bulk_insert_mappings(NewsNounsExtrcVo, valuses)

        return True
    except Exception as e:
        logger.error(e)
        # session.rollback();
        return False
    # finally:    
    #     closeSession(session)
    
# 제외 단어를 포함 json으로
def insert_stop_word(obj, session):
    logger.info("--------------- start insert_stop_word ------------------------")
    
    try :
        news_id = obj["news_sn"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        ndls_wrd = obj["ndls_wrd"]
        strndls_wrd = json.dumps(ndls_wrd, ensure_ascii=False)
        valuses = []
        
        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_sn'] = news_id
        t_obj['ndls_wrd'] = strndls_wrd
        
        session.query(NewsColctVo).filter(NewsColctVo.news_sn == t_obj['news_sn']).update(t_obj)
        
        return True
    except Exception as e:
        logger.error(e)
        # session.rollback();
        return False
    # finally:    
    #     closeSession(session)
    
    
def news_regex_main():
    try:
        start = time.time()
        session = get_session()
        session.begin()
        newRs = session.query(NewsColctVo).where(NewsColctVo.news_noun == None, NewsColctVo.news_rgsde != None, NewsColctVo.news_bdt != None);
        keyword_cur = session.query(CodeDtstmnVo).where(CodeDtstmnVo.code_usgstt == '1', CodeDtstmnVo.code_column_nm == 'kwrd_code');
        stop_words_cur = session.query(CodeDtstmnVo).where(CodeDtstmnVo.code_usgstt == '1', CodeDtstmnVo.code_column_nm == 'ndls_wrd');
        reg_ex_list = []
        stop_words = []
        keywrod_obj = dict()
        record = 0
        global page, limit, userId
            
        if bool(limit) != True: 
            raise Exception("설정 오류")
        
        for keywrod_obj in keyword_cur:
            keywrod_obj[keywrod_obj.code_dc] = keywrod_obj.code_no
            keyword = keywrod_obj.code_dc
            reg_ex_list.append(keyword)
      
        for wordObj in stop_words_cur:
            stop_words.append(wordObj.code_dc)
        
        # rmRegex = re.compile("\'|\"|{|}")
        keyword_regex = re.compile("|".join(reg_ex_list))
        
        record = newRs.limit(limit).all();
        while record :
            
            print("start loop")
            
            for row in record:
                
                news_id = row.news_sn
                news_contents = row.news_bdt
                news_contents = re.sub('[^a-z|0-9|ㄱ-ㅎ|가-힣|\s\n]', '', news_contents, flags=re.I|re.M)
                
                #기사 작성일 추출
                if row.news_rgsde :
                    newsPostDate = row.news_rgsde
                    
                #형태소 분석
                newsNouns = ko.nouns(news_contents)

                #형태소 분석을 통해 생성된 명사 개수 추출
                news_nouns_cnt_obj = ct.Counter(newsNouns)
                news_nouns_cnt_obj = dict(news_nouns_cnt_obj)
                news_year = 0
                
                #기사 작성일로 연도 추출
                if newsPostDate.year :
                    news_year = newsPostDate.year
                
                newsVo = dict()
                newsVo["news_sn"] =  news_id
                newsVo["news_nouns_cnt_obj"] =  news_nouns_cnt_obj
                newsVo["register_id"] = userId
                newsVo["updusr_id"] = userId
                
                success = insert_db_nouns(newsVo, session)

                #기사에 언급된 명사 중 등록된 명사만 개수 추출                
                if success != True:
                    raise Exception("명사 추출 jsonb 입력 오류")
                
                 # 등록된 제외단어가 포함된 뉴스는 제외 
                stop_word = [ x for x in newsNouns if x in stop_word ]
                ndls_wrd = ct.Counter(stop_word);
                
                if stop_word :
                    stopWordVo = dict()
                    stopWordVo["news_sn"] =  news_id
                    stopWordVo["ndls_wrd"] =  ndls_wrd
                    stopWordVo["register_id"] = userId
                    stopWordVo["updusr_id"] = userId
                    success = insert_stop_word(stopWordVo, session)
                    if success != True:
                        raise Exception("제외단어 등록 오류");
                    continue;
                
                if row.news_dc_code != '0000':
                    logger.info('제외 대상')
                    continue;
                
                
                x = re.findall(keyword_regex, news_contents)
                if x :
                    keywords = list(set(x))
                    for key in keywords :
                        cnt = 1
                        
                        cntVo = NewsKwrdCntVo()
                        keyword_id = keywrod_obj[key]
                        
                        vo1 = session.query(NewsKwrdCntVo).where(NewsKwrdCntVo.news_sn == news_id, NewsKwrdCntVo.kwrd_sn == keyword_id, NewsKwrdCntVo.kwrd_year == news_year).first()
                        cntVo.news_sn = news_id
                        cntVo.kwrd_year = news_year
                        cntVo.kwrd_sn = keyword_id
                        cntVo.kwrd_co = cnt
                        cntVo.register_id = userId
                        cntVo.updusr_id = userId
                        
                        session.merge(cntVo)
                        
                        vo2 = session.query(NewsKwrdYearCntVo).where(NewsKwrdYearCntVo.kwrd_sn == keyword_id, NewsKwrdYearCntVo.news_year == news_year).first()
                        newsKwrdYearCntVo = NewsKwrdYearCntVo()
                        newsKwrdYearCntVo.kwrd_sn = keyword_id
                        newsKwrdYearCntVo.news_year = news_year
                        newsKwrdYearCntVo.kwrd_sm_co = cnt if vo2 == None else vo2.kwrd_sm_co + cnt 
                        newsKwrdYearCntVo.register_id = userId
                        newsKwrdYearCntVo.rgsde = 'now()'
                        newsKwrdYearCntVo.updusr_id = userId
                        newsKwrdYearCntVo.updde = 'now()'
                        
                        session.merge(newsKwrdYearCntVo)
                            
            session.commit()
                
            print("end loop : ", page)
            page = page+1
            record = newRs.limit(limit).all();               
            
            
        session.close()
        
        logger.info("--------------- end ------------------------")
    except UnicodeDecodeError as ed : 
        session.rollback()
        print (ed)
    except Exception as e:
        session.rollback()
        logger.error(e)
    finally:
        close_session(session)

if __name__ == '__main__' :
    news_regex_main()