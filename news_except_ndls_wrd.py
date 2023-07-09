import Extract_noun as extract
from sqlalchemy.orm import scoped_session, sessionmaker
from vo.WssNewsNdlsWrdAnalsVO import WssNewsNdlsWrdAnalsVO
from vo.NewsColctVO import NewsColctVO
from vo.CodeDtstmnVO import CodeDtstmnVO
from vo.WssNewsKwrdManageVO import WssNewsKwrdManageVO
import sqlalchemy as db
import logging as logging
import json
import collections as ct
import config
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger()

# 설정 정보
url = config.jdbc_url
engine = db.create_engine(config.jdbc_url)
limit = config.regex_limit
page = config.regex_page
user_id = config.regex_user_id

# session 획득
def get_session():
    try:
        Session = scoped_session(sessionmaker(autocommit=False, autoflush=True, expire_on_commit=False, bind=engine))
        sub_session = Session()
        return sub_session
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

# def findStopWordList(session, stop_word_list, stopWordDict, news_nouns, news_url):
#     #뉴스 url - pk
#     ndls_wrd = dict()
#     ndls_wrd_list = []
    
#     #뉴스 본문이 없는 경우 
#     if news_nouns is None or news_nouns == 'null' or len(news_nouns) < 0: 
#         return None
    
#     for k in stop_word_list :
#         vo = dict(filter(lambda elem:elem[0] == k, news_nouns.items()))
#         if len(vo) > 0 :
#             ndls_wrd_list.append(vo)
            
#     if ndls_wrd_list :
#         for r in ndls_wrd_list :
#             for k, v in r.items():
#                 ndls_wrd[k] = v
#                 insertStmt = insert(WssNewsNdlsWrdAnalsVO).values(
#                     news_url = news_url,
#                     ndls_wrd_code = stopWordDict[k],
#                     ndls_wrd_cnt = v,
#                     register_id = 'system',
#                     rgsde = 'now()',
#                     updusr_id = 'system',
#                     updde = 'now()'
#                 ).on_conflict_do_nothing(
#                     constraint  = "pk_wss_news_ndls_wrd_anals"
#                 )
#                 session.execute(insertStmt)
    
#     if  len(ndls_wrd) > 0 :            
#         return ndls_wrd
#     else :
#         None
        
def findStopWordList(session, stop_word_list, stopWordDict, news_nouns, news_url, ndlsWrdList):
    #뉴스 url - pk
    ndls_wrd = dict()
    ndls_wrd_list = []
    retList = []
    
    #뉴스 본문이 없는 경우 
    if news_nouns is None or news_nouns == 'null' or len(news_nouns) < 0: 
        return None
    
    for k in stop_word_list :
        vo = dict(filter(lambda elem:elem[0] == k, news_nouns.items()))
        if len(vo) > 0 :
            ndls_wrd_list.append(vo)
            
    retList = list(map(dict, set(tuple(sorted(d.items())) for d in ndls_wrd_list)))
            
    if retList :
        for r in retList :
            for k, v in r.items():
                ndls_wrd[k] = v
                ndlsWrdList.append(dict(
                    news_url = news_url,
                    ndls_wrd_code = stopWordDict[k],
                    ndls_wrd_cnt = v,
                    register_id = 'system',
                    rgsde = 'now()',
                    updusr_id = 'system',
                    updde = 'now()'
                ))
    if  len(ndls_wrd) > 0 :            
        return ndls_wrd
    else :
        None

def insertComptAt(session, news_url) :
    try:
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['ndls_wrd_anals_compt_at'] = "Y"
        session.query(NewsColctVO).filter(NewsColctVO.news_url == t_obj['news_url']).update(t_obj)
        session.commit
        
    except Exception as e:
        raise e

def comptUpdateVO(news_url) :
    try:
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['ndls_wrd_anals_compt_at'] = "Y"
        return t_obj
        
    except Exception as e:
        raise e
    
def comptUpdateNdlsWrdVO(news_url, ndls_wrd) :
    try:
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['ndls_wrd_anals_compt_at'] = "Y"
        if (ndls_wrd is not None and len(ndls_wrd) > 0 ) : t_obj['ndls_wrd'] = json.dumps(ndls_wrd, ensure_ascii=False)
        return t_obj
        
    except Exception as e:
        raise e


def main():
    try:
        print("start")
        
        stop_word_list = []
        stopWordDict = dict()
        extract_noun = extract.Extract_noun("ndls_wrd")

        session = get_session()
        session.begin()
        
        record = 0
        global page, limit, user_id

        if bool(limit) != True:
            raise Exception("설정 오류")

        # 뉴스 목록
        news_rs = session.query(NewsColctVO).where((NewsColctVO.ndls_wrd_anals_compt_at == "N" and (NewsColctVO.ndls_wrd_anals_compt_at == "Y" and NewsColctVO.news_bdt != 'null' and NewsColctVO.news_bdt is not None)))
        # 키워드 관리번호
        manage_vo = session.query(WssNewsKwrdManageVO).where(WssNewsKwrdManageVO.use_yn == 'Y', WssNewsKwrdManageVO.delete_yn == 'N').one()
        

        # 키워드 제외 항목
        stop_word_cur = session.query(CodeDtstmnVO).where(CodeDtstmnVO.code_usgstt == '1', CodeDtstmnVO.code_column_nm == 'ndls_wrd')

        for word_obj in stop_word_cur:
            stopWordDict[word_obj.code_dc] = word_obj.code_no
            stop_word_list.append(word_obj.code_dc)

        record = news_rs.limit(limit).all()
        
        while record :
            print("start loop")
            
            ndlsWrdList = []
            comptUpdateList = []
            
            for row in record:
                
                print("row : ", row)
                news_nouns_dict = None
                news_url = row.news_url
                
                if row.news_noun is not None and len(row.news_noun) > 0 :
                    news_nouns_dict = row.news_noun
                else :
                    if row.news_bdt is None or row.news_bdt == 'null' or len(row.news_bdt) < 1 :
                        comptUpdateList.append(comptUpdateVO(news_url))
                        # insertComptAt(session, row.news_url)
                        continue
                        
                    news_nouns = extract_noun.getNouns(row.news_bdt)
                    
                    if news_nouns is None or len(news_nouns) < 1 :
                        comptUpdateList.append(comptUpdateVO(news_url))
                        # insertComptAt(session, row.news_url)
                        continue

                    news_nouns_dict = extract_noun.getNounsCntDict(news_nouns)
                
                if news_nouns_dict is not None and len(news_nouns_dict) > 0 : 
                    # 등록된 제외단어가 포함된 뉴스는 제외
                    ndls_wrd = findStopWordList(session, stop_word_list, stopWordDict, news_nouns_dict, news_url, ndlsWrdList)
                else :
                    comptUpdateList.append(comptUpdateVO(news_url))
                    continue
                
                comptUpdateList.append(comptUpdateNdlsWrdVO(news_url, ndls_wrd))
                # t_obj = dict()
                # t_obj['news_url'] = row.news_url
                # t_obj['ndls_wrd_anals_compt_at'] = "Y"
                # if (ndls_wrd is not None and len(ndls_wrd) > 0 ) : t_obj['ndls_wrd'] = json.dumps(ndls_wrd, ensure_ascii=False)
                # session.query(NewsColctVO).filter(NewsColctVO.news_url == t_obj['news_url']).update(t_obj)
                # session.commit()
            
            session.bulk_insert_mappings(WssNewsNdlsWrdAnalsVO, ndlsWrdList)
            session.bulk_update_mappings(NewsColctVO, comptUpdateList)
            session.commit()
            
                    
            print("end loop : ", page)
            page = page+1
            record = news_rs.limit(limit).all()

        session.close()

        logger.info("--------------- 종료 ------------------------")
    except UnicodeDecodeError as ed :
        session.rollback()
        print (ed)
        raise ed
    except Exception as e:
        session.rollback()
        print (e)
        raise e
    finally:
        close_session(session)

if __name__ == '__main__' :
    main()