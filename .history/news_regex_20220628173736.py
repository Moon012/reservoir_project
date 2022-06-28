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
from vo.CodeDtstmnVo import CodeDtstmnVo
from vo.NewsKwrdYearCntVo import NewsKwrdYearCntVo
import sqlalchemy as db
import logging as logging
import json
import collections as ct
import time

logger = logging.getLogger()

# 설정 정보
with open('./config/jdbc.json') as f1, open('./config/config.json') as f2:
    jdbcConf = json.load(f1)
    conf = json.load(f2)
    url = jdbcConf['url']
    engine = db.create_engine(**jdbcConf)
    limit = conf["limit"]
    page = conf["page"]
    user_id = conf["user_id"]

# mecab = Mecab("C:\\mecab\\mecab-ko-dic")
# kkma = Kkma()
# ha = Hannanum()
ko = Komoran()
# okt = Okt()

# session 획득
def getSession():
    try:
        Session = scoped_session(sessionmaker(autocommit=False, autoflush=True, expire_on_commit=False, bind=engine))
        session = Session()
        return session
    except Exception as e: 
        logger.error(e)

# session 종료
def closeSession(session):
    try:
        session.close()
    except Exception as e: 
        logger.error(e)
    finally:
        if session is not None:
            session.close()
            
# 형태소 분석기를 통한 명사를 json 채로 저장
def insertDbNouns(obj, session):
    try :
        newsid = obj["news_sn"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        nounsObj = obj["newsNounsCntObj"]
        strNounsObj = json.dumps(nounsObj, ensure_ascii=False)
        valuses = []
        
        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_sn'] = newsid
        t_obj['news_noun'] = strNounsObj
        
        session.query(NewsColctVo).filter(NewsColctVo.news_sn == t_obj['news_sn']).update(t_obj)
        
        for key, value in nounsObj.items():
            t_nounsObj = dict()
            t_nounsObj['news_sn'] = newsid
            t_nounsObj['register_id'] = register_id
            t_nounsObj['updusr_id'] = updusr_id
            t_nounsObj['news_nouns'] = key
            t_nounsObj['news_nouns_co'] = value
            valuses.append(t_nounsObj)

        session.bulk_insert_mappings(NewsNounsExtrcVo, valuses)

        return True
    except Exception as e:
        logger.error(e)
        # session.rollback();
        return False
    # finally:    
    #     closeSession(session)
    
# 제외 단어를 포함 json으로
def insertStopWords(obj, session):
    logger.info("--------------- start insertStopWords ------------------------")
    
    try :
        newsid = obj["news_sn"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        ndls_wrd = obj["ndls_wrd"]
        strndls_wrd = json.dumps(ndls_wrd, ensure_ascii=False)
        valuses = []
        
        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_sn'] = newsid
        t_obj['ndls_wrd'] = strndls_wrd
        
        session.query(NewsColctVo).filter(NewsColctVo.news_sn == t_obj['news_sn']).update(t_obj)
        
        return True
    except Exception as e:
        logger.error(e)
        # session.rollback();
        return False
    # finally:    
    #     closeSession(session)
    
    
def main():
    try:
        start = time.time()
        session = getSession()
        session.begin()
        newRs = session.query(NewsColctVo).where(NewsColctVo.news_noun == None, NewsColctVo.news_rgsde != None, NewsColctVo.news_bdt != None);
        keywordCur = session.query(CodeDtstmnVo).where(CodeDtstmnVo.code_usgstt == '1', CodeDtstmnVo.code_column_nm == 'kwrd_code');
        stop_words_cur = session.query(CodeDtstmnVo).where(CodeDtstmnVo.code_usgstt == '1', CodeDtstmnVo.code_column_nm == 'ndls_wrd');
        regExList = []
        stop_words = []
        keywordObj = dict()
        record = 0
        global page, limit, user_id
            
        if bool(limit) != True: 
            raise Exception("설정 오류")
        
        for keywrodObj in keywordCur:
            keywordObj[keywrodObj.code_dc] = keywrodObj.code_no
            keyword = keywrodObj.code_dc
            regExList.append(keyword)
      
        for wordObj in stop_words_cur:
            stop_words.append(wordObj.code_dc)
        
        # rmRegex = re.compile("\'|\"|{|}")
        keywordRegex = re.compile("|".join(regExList))
        
        record = newRs.limit(limit).all();
        while record :
            
            print("start loop")
            
            for row in record:
                
                newsId = row.news_sn
                newsContents = row.news_bdt
                newsContents = re.sub('[^a-z|0-9|ㄱ-ㅎ|가-힣|\s\n]', '', newsContents, flags=re.I|re.M)
                
                #기사 작성일 추출
                if row.news_rgsde :
                    newsPostDate = row.news_rgsde
                    
                #형태소 분석
                newsNouns = ko.nouns(newsContents)

                #형태소 분석을 통해 생성된 명사 개수 추출
                newsNounsCntObj = ct.Counter(newsNouns)
                newsNounsCntObj = dict(newsNounsCntObj)
                newsYear = 0
                
                #기사 작성일로 연도 추출
                if newsPostDate.year :
                    newsYear = newsPostDate.year
                
                newsVo = dict()
                newsVo["news_sn"] =  newsId
                newsVo["newsNounsCntObj"] =  newsNounsCntObj
                newsVo["register_id"] = user_id
                newsVo["updusr_id"] = user_id
                
                success = insertDbNouns(newsVo, session)

                #기사에 언급된 명사 중 등록된 명사만 개수 추출                
                if success != True:
                    raise Exception("명사 추출 jsonb 입력 오류")
                
                 # 등록된 제외단어가 포함된 뉴스는 제외 
                stopWords = [ x for x in newsNouns if x in stop_words ]
                ndls_wrd = ct.Counter(stopWords);
                
                if stopWords :
                    stopWordVo = dict()
                    stopWordVo["news_sn"] =  newsId
                    stopWordVo["ndls_wrd"] =  ndls_wrd
                    stopWordVo["register_id"] = user_id
                    stopWordVo["updusr_id"] = user_id
                    success = insertStopWords(stopWordVo, session)
                    if success != True:
                        raise Exception("제외단어 등록 오류");
                    continue;
                
                if row.news_dc_code != '0000':
                    logger.info('제외 대생')
                    continue;
                
                
                x = re.findall(keywordRegex, newsContents)
                if x :
                    keywords = list(set(x))
                    for key in keywords :
                        cnt = 1
                        
                        cntVo = NewsKwrdCntVo()
                        keywordId = keywordObj[key]
                        
                        vo1 = session.query(NewsKwrdCntVo).where(NewsKwrdCntVo.news_sn == newsId, NewsKwrdCntVo.kwrd_sn == keywordId, NewsKwrdCntVo.kwrd_year == newsYear).first()
                        cntVo.news_sn = newsId
                        cntVo.kwrd_year = newsYear
                        cntVo.kwrd_sn = keywordId
                        cntVo.kwrd_co = cnt
                        cntVo.register_id = user_id
                        cntVo.updusr_id = user_id
                        
                        session.merge(cntVo)
                        
                        vo2 = session.query(NewsKwrdYearCntVo).where(NewsKwrdYearCntVo.kwrd_sn == keywordId, NewsKwrdYearCntVo.news_year == newsYear).first()
                        newsKwrdYearCntVo = NewsKwrdYearCntVo()
                        newsKwrdYearCntVo.kwrd_sn = keywordId
                        newsKwrdYearCntVo.news_year = newsYear
                        newsKwrdYearCntVo.kwrd_sm_co = cnt if vo2 == None else vo2.kwrd_sm_co + cnt 
                        newsKwrdYearCntVo.register_id = user_id
                        newsKwrdYearCntVo.rgsde = 'now()'
                        newsKwrdYearCntVo.updusr_id = user_id
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
        closeSession(session)

if __name__ == '__main__' :
    main()