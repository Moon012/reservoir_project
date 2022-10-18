import re
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import UniqueViolation
from konlpy.tag import Mecab, Kkma, Okt, Hannanum, Komoran
from sqlalchemy.orm import scoped_session, sessionmaker
from vo.NewsKwrdCntVO import NewsKwrdCntVO
from vo.NewsNounsExtrcVO import NewsNounsExtrcVO
from vo.NewsColctVO import NewsColctVO
from vo.CodeDtstmnVO import CodeDtstmnVO
from vo.WssNewsKwrdDalyCntVO import WssNewsKwrdDalyCntVO
from vo.WssNewsKwrdManageVO import WssNewsKwrdManageVO
from vo.WssNewsColctKwrdInfoVO import WssNewsColctKwrdInfoVO
from vo.WssNewsColctKwrdVO import WssNewsColctKwrdVO
from vo.WssNewsAnalsKwrdVO import WssNewsAnalsKwrdVO
import sqlalchemy as db
import logging as logging
import json
import collections as ct
import config

logger = logging.getLogger()

# 설정 정보
url = config.jdbc_url
engine = db.create_engine(config.jdbc_url)
limit = config.regex_limit
page = config.regex_page
user_id = config.regex_user_id

ko = Komoran()

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

# 형태소 분석기를 통한 명사를 json 채로 저장
def insert_db_nouns(obj, session):
    try :
        news_url = obj["news_url"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        nouns_obj = obj["news_nouns_cnt_obj"]
        str_nouns_obj = json.dumps(nouns_obj, ensure_ascii=False)
        valuses = []

        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['news_noun'] = str_nouns_obj

        session.query(NewsColctVO).filter(NewsColctVO.news_url == t_obj['news_url']).update(t_obj)

        for key, value in nouns_obj.items():
            NewsNounsExtrcCntVO = session.query(NewsNounsExtrcVO).where(NewsNounsExtrcVO.news_url == news_url).first()

            if NewsNounsExtrcCntVO is None :
                session.add(
                    NewsNounsExtrcVO(
                        news_url = news_url,
                        news_nouns = key,
                        news_nouns_co = value,
                        register_id = register_id,
                        rgsde = 'now()',
                        updusr_id = register_id,
                        updde = 'now()',
                    )
                )
            else :
                pass

        return True
    except Exception as e:
        logger.error(e)
        # session.rollback()
        return False
    # finally:
    #     close_session(session)

# 제외 단어를 포함 json으로
def insert_stop_word(obj, session):
    try :
        news_url = obj["news_url"]
        register_id = obj["register_id"]
        updusr_id = obj["updusr_id"]
        ndls_wrd = obj["ndls_wrd"]
        strndls_wrd = json.dumps(ndls_wrd, ensure_ascii=False)
        valuses = []

        # 형태소가 분석된 jsonb의 데이터를 입력
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['ndls_wrd'] = strndls_wrd

        session.query(NewsColctVO).filter(NewsColctVO.news_url == t_obj['news_url']).update(t_obj)

        return True
    except Exception as e:
        logger.error(e)
        # session.rollback()
        return False
    # finally:
    #     close_session(session)

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

def getStopWord(session):
    stop_word_list = []

    # 키워드 제외 항목
    stop_word_cur = session.query(CodeDtstmnVO).where(CodeDtstmnVO.code_usgstt == '1', CodeDtstmnVO.code_column_nm == 'ndls_wrd')

    for word_obj in stop_word_cur:
        stop_word_list.append(word_obj.code_dc)

    return stop_word_list

def news_regex_main():
    try:
        print("start")

        session = get_session()
        session.begin()

        # 뉴스 목록
        news_rs = session.query(NewsColctVO).where(NewsColctVO.news_noun == None, NewsColctVO.news_rgsde != None, NewsColctVO.news_bdt != None)
        # 키워드 관리번호
        manage_vo = session.query(WssNewsKwrdManageVO).where(WssNewsKwrdManageVO.use_yn == 'Y', WssNewsKwrdManageVO.delete_yn == 'N').one()

        record = 0
        global page, limit, user_id

        if bool(limit) != True:
            raise Exception("설정 오류")

        keyword_obj = getKeywordObj(session, manage_vo)
        stop_word_list = getStopWord(session)

        # rmRegex = re.compile("\'|\"|{|}")
        # keyword_regex = re.compile("|".join(regex_list))

        record = news_rs.limit(limit).all()
        while record :

            print("start loop")

            for row in record:

                #뉴스 url - pk
                news_url = row.news_url
                #뉴스 내용
                news_contests = row.news_bdt
                #한글만 추출
                news_contests = re.sub('[^a-z|0-9|ㄱ-ㅎ|가-힣|\s\n]', '', news_contests, flags=re.I|re.M)

                #기사 작성일 추출
                if row.news_rgsde :
                    news_post_date = row.news_rgsde

                #형태소 분석
                news_nouns = ko.nouns(news_contests)

                #형태소 분석을 통해 생성된 명사 개수 추출
                news_nouns_cnt_obj = ct.Counter(news_nouns)
                news_nouns_cnt_obj = dict(news_nouns_cnt_obj)
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

                success = insert_db_nouns(newsVO, session)

                #기사에 언급된 명사 중 등록된 명사만 개수 추출
                if success != True:
                    raise Exception("명사 추출 jsonb 입력 오류")

                # 등록된 제외단어가 포함된 뉴스는 제외
                stop_word_list = [ x for x in news_nouns if x in stop_word_list ]
                ndls_wrd = ct.Counter(stop_word_list)

                if stop_word_list :
                    stop_word_vo = dict()
                    stop_word_vo["news_url"] =  news_url
                    stop_word_vo["ndls_wrd"] =  ndls_wrd
                    stop_word_vo["register_id"] = user_id
                    stop_word_vo["updusr_id"] = user_id
                    success = insert_stop_word(stop_word_vo, session)
                    if success != True:
                        raise Exception("제외단어 등록 오류")
                    continue

                if row.news_dc_code != '0000':
                    logger.info('제외 대상')
                    continue

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
                            session.add(
                                NewsKwrdCntVO(
                                    news_url        = news_url,
                                    kwrd_manage_no  = manage_vo.kwrd_manage_no,
                                    kwrd_colct_code = ivo.kwrd_colct_code,
                                    kwrd_code       = keywordObj[keyword],
                                    kwrd_co         = cnt,
                                    register_id     = user_id,
                                    rgsde           = 'now()',
                                    updusr_id       = user_id,
                                    updde           = 'now()'
                                )
                            )

                            dalyCntvo = session.query(WssNewsKwrdDalyCntVO).where(WssNewsKwrdDalyCntVO.news_year == news_year, WssNewsKwrdDalyCntVO.news_month == news_month,
                                                                                  WssNewsKwrdDalyCntVO.news_day == news_day).first()

                            newsKwrdDalyCntVO = WssNewsKwrdDalyCntVO()
                            newsKwrdDalyCntVO.news_year = news_year
                            newsKwrdDalyCntVO.news_month = news_month
                            newsKwrdDalyCntVO.news_day = news_day
                            newsKwrdDalyCntVO.kwrd_sm_co = cnt if dalyCntvo == None else dalyCntvo.kwrd_sm_co + cnt
                            newsKwrdDalyCntVO.register_id = user_id
                            if newsKwrdDalyCntVO.rgsde == None:
                                newsKwrdDalyCntVO.rgsde = 'now()'
                            newsKwrdDalyCntVO.updusr_id = user_id
                            newsKwrdDalyCntVO.updde = 'now()'

                            session.merge(newsKwrdDalyCntVO)

            session.commit()

            print("end loop : ", page)
            page = page+1
            record = news_rs.limit(limit).all()

        session.close()

        logger.info("--------------- 종료 ------------------------")
    except UnicodeDecodeError as ed :
        session.rollback()
        print (ed)
        raise ed;
    except Exception as e:
        session.rollback()
        print (e)
        raise e;
    finally:
        close_session(session)

if __name__ == '__main__' :
    news_regex_main()