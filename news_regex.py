import Extract_noun as extract
from sqlalchemy.orm import scoped_session, sessionmaker
from vo.NewsKwrdCntVO import NewsKwrdCntVO
from vo.NewsNounsExtrcVO import NewsNounsExtrcVO
from vo.NewsColctVO import NewsColctVO
from vo.WssNewsKwrdManageVO import WssNewsKwrdManageVO
from vo.WssNewsColctKwrdInfoVO import WssNewsColctKwrdInfoVO
from vo.WssNewsColctKwrdVO import WssNewsColctKwrdVO
from vo.WssNewsAnalsKwrdVO import WssNewsAnalsKwrdVO
import sqlalchemy as db
import logging as logging
import json
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

def comptUpdateNounsVO(obj):
    try :
        news_url = obj["news_url"]
        nouns_obj = obj["news_nouns_cnt_obj"]
        str_nouns_obj = None
        if len(nouns_obj) :
            str_nouns_obj = json.dumps(nouns_obj, ensure_ascii=False)
        
        valuses = []

        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['news_noun'] = str_nouns_obj
        t_obj['noun_anals_compt_at'] = "Y"

        return t_obj
            
    except Exception as e:
        raise e
def getExtrcCntUpdateVO(obj):
    try :
        news_url = obj["news_url"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        nouns_obj = obj["news_nouns_cnt_obj"]
        valuses = []

        for key, value in nouns_obj.items():
            valuses.append(dict(
                news_url = news_url,
                news_nouns = key,
                news_nouns_co = value,
                register_id = register_id,
                rgsde = 'now()',
                updusr_id = register_id,
                updde = 'now()'
            ))
            
        return list(map(dict, set(tuple(sorted(d.items())) for d in valuses)))
    except Exception as e:
        raise e

def getKeywordObj(session, manage_vo):
    keyword_obj = dict()

    # 검색 키워드 항목 -- 전체
    search_keyword_cur = session.query(WssNewsColctKwrdVO).where(WssNewsColctKwrdVO.kwrd_manage_no == manage_vo.kwrd_manage_no)
    for search_keyword in search_keyword_cur:

        try:
            keyword_obj[search_keyword.kwrd_colct_code]
        except KeyError :
            keyword_obj[search_keyword.kwrd_colct_code] = dict()

        keyword_cur = session.query(WssNewsAnalsKwrdVO).where(WssNewsAnalsKwrdVO.kwrd_manage_no == search_keyword.kwrd_manage_no,
                                                              WssNewsAnalsKwrdVO.kwrd_colct_code == search_keyword.kwrd_colct_code)
        for keyword in keyword_cur:
            try:
                keyword_obj[keyword.kwrd_colct_code][keyword.kwrd_nm]
            except KeyError :
                keyword_obj[keyword.kwrd_colct_code][keyword.kwrd_nm] = dict()

            keyword_obj[keyword.kwrd_colct_code][keyword.kwrd_nm] = keyword.kwrd_code

    return keyword_obj

# 형태소 분석기를 통한 명사를 json 채로 저장
def insert_db_nouns(obj, session):
    try :
        news_url = obj["news_url"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        nouns_obj = obj["news_nouns_cnt_obj"]
        str_nouns_obj = None
        if len(nouns_obj) :
            str_nouns_obj = json.dumps(nouns_obj, ensure_ascii=False)
        
        valuses = []

        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['news_noun'] = str_nouns_obj
        t_obj['noun_anals_compt_at'] = "Y"

        session.query(NewsColctVO).filter(NewsColctVO.news_url == t_obj['news_url']).update(t_obj)

        for key, value in nouns_obj.items():
            
            insert_extrc_stmt  = insert(NewsNounsExtrcVO).values(
                news_url = news_url,
                news_nouns = key,
                news_nouns_co = value,
                register_id = register_id,
                rgsde = 'now()',
                updusr_id = register_id,
                updde = 'now()'
            ).on_conflict_do_nothing(
                constraint  = "pk_wss_news_nouns_extrc"
            )
            
            session.execute(insert_extrc_stmt)
            
    except Exception as e:
        raise e

def insertComptAt(session, news_url) :
    try:
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['noun_anals_compt_at'] = "Y"
        session.query(NewsColctVO).filter(NewsColctVO.news_url == t_obj['news_url']).update(t_obj)
        session.commit
        
    except Exception as e:
        raise e
    
def comptUpdateVO(news_url) :
    try:
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['noun_anals_compt_at'] = "Y"
        return t_obj
        
    except Exception as e:
        raise e
    

def news_regex_main():
    try:
        print("start")

        session = get_session()
        session.begin()

        # 뉴스 목록
        news_rs = session.query(NewsColctVO).where((NewsColctVO.noun_anals_compt_at == "N" and (NewsColctVO.noun_anals_compt_at == "Y" and NewsColctVO.news_bdt != 'null' and NewsColctVO.news_bdt is not None)))
        # 키워드 관리번호
        manage_vo = session.query(WssNewsKwrdManageVO).where(WssNewsKwrdManageVO.use_yn == 'Y', WssNewsKwrdManageVO.delete_yn == 'N').one()

        record = 0
        global page, limit, user_id

        if bool(limit) != True:
            raise Exception("설정 오류")

        keyword_obj = getKeywordObj(session, manage_vo)
        
        extract_noun = extract.Extract_noun("nouns");

        record = news_rs.limit(limit).all()
        while record :

            print("start loop")
            nounsUpdateVOList = []
            comptUpdateList = []
            nounsAnalsList = []
            nounsExtrcVOList = []

            for row in record:
                print("row : ", row)

                #뉴스 url - pk
                news_url = row.news_url
                #뉴스 내용
                news_contents = row.news_bdt

                #기사 작성일 추출
                if row.news_rgsde :
                    news_post_date = row.news_rgsde
                    
                if news_contents is None or news_contents == 'null' or len(news_contents) < 0:
                     # 명사 분석 데이터 없는 경우
                    # insertComptAt(session, row.news_url)
                    comptUpdateList.append(comptUpdateVO(news_url))
                    continue

                #형태소 분석
                news_nouns = extract_noun.getNouns(news_contents)
                
                if news_nouns is None or len(news_nouns) < 1 :
                    # insertComptAt(session, news_url)
                    comptUpdateList.append(comptUpdateVO(news_url))
                    continue

                #형태소 분석을 통해 생성된 명사 개수 추출
                news_nouns_cnt_obj = extract_noun.getNounsCntDict(news_nouns)
                
                if news_nouns_cnt_obj is None or len(news_nouns_cnt_obj) < 1 :
                    comptUpdateList.append(comptUpdateVO(news_url))
                    # insertComptAt(session, news_url)
                    continue
                
                news_year = ''
                news_month = ''
                news_day = ''
                
                #기사 작성일로 연도 추출 - 통계 테이블
                if news_post_date.year :
                    news_year = str(news_post_date.year)
                #기사 작성일로 월 추출 - 통계 테이블
                if news_post_date.month :
                    news_month = str(news_post_date.month).zfill(2)
                #기사 작성일로 일 추출 - 통계 테이블
                if news_post_date.year :
                    news_day = str(news_post_date.day).zfill(2)

                newsVO = dict()
                newsVO["news_url"] =  news_url
                newsVO["news_nouns_cnt_obj"] =  news_nouns_cnt_obj
                newsVO["register_id"] = user_id
                newsVO["updusr_id"] = user_id

                # 키워드 항목
                cloctKwrdInfoCur = session.query(WssNewsColctKwrdInfoVO).where(WssNewsColctKwrdInfoVO.news_url == news_url,
                                                                               WssNewsColctKwrdInfoVO.kwrd_manage_no == manage_vo.kwrd_manage_no)
                for ivo in cloctKwrdInfoCur :
                    keywordObj = keyword_obj[ivo.kwrd_colct_code]

                    for keyword in keywordObj :
                        try :
                            cnt = news_nouns_cnt_obj[keyword]
                        except KeyError :
                            continue

                        if (cnt is not None) :
                            nounsAnalsList.append(dict(
                                news_url        = news_url,
                                kwrd_manage_no  = manage_vo.kwrd_manage_no,
                                kwrd_colct_code = ivo.kwrd_colct_code,
                                kwrd_code       = keywordObj[keyword],
                                kwrd_co         = cnt,
                                register_id     = user_id,
                                rgsde           = 'now()',
                                updusr_id       = user_id,
                                updde           = 'now()',
                                news_year       = news_year,
                                news_month      = news_month,
                                news_day        = news_day,
                            ))
                            # insert_Kwrd_stmt = insert(NewsKwrdCntVO).values(
                            #     news_url        = news_url,
                            #     kwrd_manage_no  = manage_vo.kwrd_manage_no,
                            #     kwrd_colct_code = ivo.kwrd_colct_code,
                            #     kwrd_code       = keywordObj[keyword],
                            #     kwrd_co         = cnt,
                            #     register_id     = user_id,
                            #     rgsde           = 'now()',
                            #     updusr_id       = user_id,
                            #     updde           = 'now()',
                            #     news_year       = news_year,
                            #     news_month      = news_month,
                            #     news_day        = news_day,
                            # ).on_conflict_do_nothing(
                            #     constraint  = "pk_wss_news_kwrd_cnt"
                            # )
                            
                            # session.execute(insert_Kwrd_stmt)
                            
                nounsUpdateVOList.append(comptUpdateNounsVO(newsVO))
                nounsExtrcVOList = getExtrcCntUpdateVO(newsVO)
            
            session.bulk_insert_mappings(NewsKwrdCntVO, nounsAnalsList)
            session.bulk_insert_mappings(NewsNounsExtrcVO, nounsExtrcVOList)
            session.bulk_update_mappings(NewsColctVO, nounsUpdateVOList)
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
    news_regex_main()