import re
import threading as thread
import ThreadHelpCount as THC
import time
import Extract_noun as extract
from sqlalchemy.orm import scoped_session, sessionmaker
from vo.NewsColctVO import NewsColctVO
from vo.WssNewsAreaAnalsVO import WssNewsAreaAnalsVO
from vo.WssCtprvnVO import WssCtprvnVO
from vo.WssAdmSgg import WssAdmSgg
import collections as ct
import sqlalchemy as db
import logging as logging
import config
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger()

# 설정 정보
url = config.jdbc_url
engine = db.create_engine(config.jdbc_url, echo=False)

extract_noun = extract.Extract_noun("area")

threadCnt = 4
tcount = THC.ThreadHelpCount(threadCnt)
lock = thread.Lock()


# session 획득
def get_session():
    try:
        Session = scoped_session(sessionmaker(autocommit=False, autoflush=True, expire_on_commit=True, bind=engine))
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
            
def selectCtprvnCd(session):
    # 시도 코드
    ctprvnList = session.query(WssCtprvnVO).all();
    retList = []
    
    for vo in ctprvnList:
        retList.append({"code": vo.bjcd2, "name" : vo.name})
        strLen = len(vo.name)
        
        if (strLen == 3) :
            #강원도 -> 강원
            str = vo.name[0:2]
        elif (strLen == 4) :
            #충청남도 -> 충남
            str = vo.name[0] + vo.name[2]
        elif (strLen > 4 and vo.name != "제주특별자치도") :
            str = vo.name[0:2] + vo.name[len(vo.name):len(vo.name)]
        elif (vo.name == "제주특별자치도") :
            str = "제주시"
            
        if str is not None : 
            retList.append({"code": vo.bjcd2, "name" : str})
            
    return list(map(dict, set(tuple(sorted(d.items())) for d in retList)))

def selectAdmSggCd(session, code):
    # 시군구 코드
    sggList = None
    
    if  code is not None :
        sggList = session.query(WssAdmSgg).where(WssAdmSgg.adm_sect_c.like(code+"%")).all()
    else :
        sggList = session.query(WssAdmSgg).all()
        
    sggCountList = []
    retList = []
    nameList = [];
    global ct
    
    for sgg in sggList : 
        nameList.append(sgg.sgg_nm)
        sggCountList.append({
            "gid" : sgg.gid,
            "adm_sect_c" : sgg.adm_sect_c,
            "sgg_nm" : sgg.sgg_nm,
            "sgg_oid" : sgg.sgg_oid,
            "col_adm_se" : sgg.col_adm_se})
    
    for k, v in ct.Counter(nameList).items() : 
        for c_vo in sggCountList :
            if k == c_vo['sgg_nm']: 
                c_vo['name_count'] = v
    
    for vo in sggCountList :
        p1_str = ""
        p1_1_str = ""
        p2_str = ""
        p2_1_str = ""
        
        if (re.match(flags=re.I|re.M, pattern='[\s]', string=vo['sgg_nm'])) :
            p1_str = vo['sgg_nm'].split(' ')[0]
            p2_str = vo['sgg_nm'].split(' ')[1]
        else :
            p1_str = vo['sgg_nm']
            
        if (len(p1_str) > 2 and len(p1_str) < 5) :
            # ex) 영등포구 -> 영등포
            p1_1_str =  p1_str[:len(p1_str)-1]
        elif (len(p1_str) > 5) :
            #세종특별자치시
            p1_1_str =  p1_str[0:2]
        
        if (len(p2_str) > 2) :
            # ex) 분당구 -> 분당
            p2_1_str =  p2_str[0:2]
        
        retList.append({ "code": vo['adm_sect_c'], "part_name_1" : p1_str, "part_name_1_1": p1_1_str, "part_name_2": p2_str, "part_name_2_1": p2_1_str, "name_count" : vo['name_count']})
    
        
    return list(map(dict, set(tuple(sorted(d.items())) for d in retList)))

def findCptCode(news_nouns, ctpList) :
    pickedCptList = []
    news_noun = news_nouns;
    
    for ctp in ctpList:            
        vo = dict(filter(lambda elem:elem[0] == ctp['name'], news_noun.items()))
        if len(vo) > 0 :
            pickedCptList.append(ctp)
    
    return pickedCptList

def checkSgg(sgg, news_noun) :
    fvo = dict(filter(lambda elem: (elem[0] == sgg['part_name_1'] or elem[0] == sgg['part_name_1_1']), news_noun.items()))
    svo = dict(filter(lambda elem: (elem[0] == sgg['part_name_2'] or elem[0] == sgg['part_name_2_1']), news_noun.items()))
    
    if len(sgg['part_name_2']) < 1 and len(fvo) > 0:
        return sgg
    elif len(sgg['part_name_2']) > 0 and len(fvo) > 0 and len(svo) > 0 :
        return sgg

    return None

def findSggCode(session, news_nouns, selectedCptList) :
    pickedSggList = []
    news_noun = news_nouns
    
    if selectedCptList is not None :
        for ctp in selectedCptList :
            #시군구 코드(+축약 단어)
            sggList = selectAdmSggCd(session, ctp['code']);
            for sgg in sggList :
                vo = checkSgg(sgg, news_noun)
                if vo is not None : 
                    pickedSggList.append(vo)
    else :        
        sggList = selectAdmSggCd(session, None);
        for sgg in sggList :
            #시도 값이 없고 시군구목록으로 조회시 중구,강서구 등은 특정하기 어려움 뺀다
            if sgg.count < 2 :
                vo = checkSgg(sgg, news_noun)
                if vo is not None : 
                    pickedSggList.append(vo)
                    
    return list(map(dict, set(tuple(sorted(d.items())) for d in pickedSggList)))

def insertComptAt(session, news_url) :
    try:
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['area_anals_compt_at'] = "Y"
        session.query(NewsColctVO).filter(NewsColctVO.news_url == t_obj['news_url']).update(t_obj)
        session.commit
        
    except Exception as e:
        raise e

def comptUpdateVO(news_url) :
    try:
        t_obj = dict()
        t_obj['news_url'] = news_url
        t_obj['area_anals_compt_at'] = "Y"
        return t_obj
        
    except Exception as e:
        raise e
    
def _main():
    try:
        print("지역 분석 시작")
        global tcount
        
        extract_noun = extract.Extract_noun("area")
        
        limit = config.regex_limit
        # page = config.regex_page
        page = 0
        user_id = 'system'

        session = get_session()
        session.begin()
        
        # 지역 분석 뉴스 목록
        news_rs = session.query(NewsColctVO).where((NewsColctVO.area_anals_compt_at == "N" and (NewsColctVO.area_anals_compt_at == "Y" and NewsColctVO.news_bdt != 'null' and NewsColctVO.news_bdt is not None)))
        
        #시도 코드(+축약 단어)
        ctpList = selectCtprvnCd(session)

        # page = tcount.borrowKey()
        #지역 분석 뉴스 조회        
        record = news_rs.limit(limit)
        i = 1
        
        while record :
            ctprvnAnalsList = []
            sggAnalsList = []
            comptUpdateList = []
            
            for row in record : 
                vo = None
                news_url = row.news_url
                news_nouns_dict = None
                # print("thread: ", threadN, " " "row : ", i, " -> ", row)
                print("row : ", i, " -> ", row)
                i=i+1
                
                #형태소 분석된 값이 없으면 지역 분석 X -> 분석 완료처리
                if row.news_noun is not None and len(row.news_noun) > 0 : 
                    news_nouns_dict = row.news_noun
                else :
                    if row.news_bdt is None or row.news_bdt == 'null' or len(row.news_bdt) < 1 :
                        # insertComptAt(session, news_url)
                        comptUpdateList.append(comptUpdateVO(news_url))
                        continue
                    
                    # lock.acquire()    
                    news_nouns = extract_noun.getNouns(row.news_bdt)
                    # lock.release()
                    
                    if news_nouns is None or len(news_nouns) < 1 :
                        # insertComptAt(session, news_url)
                        comptUpdateList.append(comptUpdateVO(news_url))
                        continue
                    
                    news_nouns_dict = extract_noun.getNounsCntDict(news_nouns)
                    
                    if news_nouns_dict is None or len(news_nouns_dict) < 1 :
                        # insertComptAt(session, news_url)
                        comptUpdateList.append(comptUpdateVO(news_url))
                        continue
                    
                #시도 코드 찾음
                selectedCptList = findCptCode(news_nouns_dict, ctpList)
                selectedSggList = []
                
                if len(selectedCptList) > 0 :
                    #시군구 코드 찾음
                    selectedSggList = findSggCode(session, news_nouns_dict, selectedCptList)
                    
                    
                if len(selectedSggList) > 0 :
                    for sgg in selectedSggList :
                        sggAnalsList.append(dict(
                            news_url = news_url,
                            ctprvn_cd = sgg['code'][0:2],
                            adm_sect_c = sgg['code'],
                            register_id = user_id,
                            rgsde = 'now()',
                            updusr_id = user_id,
                            updde = 'now()'
                        ))
                
                elif len(selectedCptList) > 0 :
                    exceptDuplicateCpt = list({cpt["code"] : cpt for cpt in selectedCptList}.values())
                    
                    for ctp in exceptDuplicateCpt :
                        ctprvnAnalsList.append(dict(
                            news_url = news_url,
                            ctprvn_cd = ctp['code'],
                            adm_sect_c = '00000',
                            register_id = user_id,
                            rgsde = 'now()',
                            updusr_id = user_id,
                            updde = 'now()'
                        ))
                        
                comptUpdateList.append(comptUpdateVO(news_url))
            
            session.bulk_insert_mappings(WssNewsAreaAnalsVO, ctprvnAnalsList)
            session.bulk_insert_mappings(WssNewsAreaAnalsVO, sggAnalsList)
            session.bulk_update_mappings(NewsColctVO, comptUpdateList)
            session.commit()
            # tcount.returnKey()
            # while True:
            #     page = tcount.borrowKey()
            #     if page == 0 : 
            #         time.sleep(1)
            #     else :
            #         break
                
            print("end loop : ")
            print("page(borrowKey) : ", page)
            record = news_rs.limit(limit).all()
        
        print("지역 분석 종료")

    except Exception as e:
        session.rollback()
        print (e)
        raise e
    finally:
        close_session(session)
        
def _test(threadN, threadCnt):
    try:
        print("지역 분석 시작")
        limit = config.regex_limit

        page = 0
        user_id = 'system'

        session = get_session()
        session.begin()
        
        with lock :
            # 지역 분석 뉴스 목록
            news_rs = session.query(NewsColctVO).where(NewsColctVO.area_anals_compt_at == "N").order_by(NewsColctVO.news_sn)
            
            cnt = session.query(NewsColctVO).where(NewsColctVO.area_anals_compt_at == "N").count()
            print("cnt" , cnt)
        
        #시도 코드(+축약 단어)
        ctpList = selectCtprvnCd(session)
        
        offset = threadN*limit
        print("page : ", offset)
        
        #지역 분석 뉴스 조회        
        record = news_rs.offset(offset).limit(limit)
        
        i = 1
        
        while record :
            ctprvnAnalsList = []
            sggAnalsList = []
            comptUpdateList = []
            
            for row in record : 
                vo = None
                news_url = row.news_url
                news_nouns_dict = None
                # if threadN == 1 :
                #     print("news_url : ", news_url)
                #     print("thread: ", threadN, " " "row : ", i, " -> ", row,)
                #     i=i+1
                
                #형태소 분석된 값이 없으면 지역 분석 X -> 분석 완료처리
                if row.news_noun is not None and len(row.news_noun) > 0 : 
                    news_nouns_dict = row.news_noun
                else :
                    if row.news_bdt is None or row.news_bdt == 'null' or len(row.news_bdt) < 1 :
                        comptUpdateList.append(comptUpdateVO(news_url))
                        continue
                    
                    lock.acquire()    
                    news_nouns = extract_noun.getNouns(row.news_bdt)
                    lock.release()
                    
                    if news_nouns is None or len(news_nouns) < 1 :
                        comptUpdateList.append(comptUpdateVO(news_url))
                        continue
                    
                    news_nouns_dict = extract_noun.getNounsCntDict(news_nouns)
                    
                    if news_nouns_dict is None or len(news_nouns_dict) < 1 :
                        # insertComptAt(session, news_url)
                        comptUpdateList.append(comptUpdateVO(news_url))
                        continue
                    
                #시도 코드 찾음
                selectedCptList = findCptCode(news_nouns_dict, ctpList)
                selectedSggList = []
                
                if len(selectedCptList) > 0 :
                    #시군구 코드 찾음
                    selectedSggList = findSggCode(session, news_nouns_dict, selectedCptList)
                    
                    
                if len(selectedSggList) > 0 :
                    for sgg in selectedSggList :
                        sggAnalsList.append(dict(
                            news_url = news_url,
                            ctprvn_cd = sgg['code'][0:2],
                            adm_sect_c = sgg['code'],
                            register_id = user_id,
                            rgsde = 'now()',
                            updusr_id = user_id,
                            updde = 'now()'
                        ))
                
                elif len(selectedCptList) > 0 :
                    exceptDuplicateCpt = list({cpt["code"] : cpt for cpt in selectedCptList}.values())
                    
                    for ctp in exceptDuplicateCpt :
                        ctprvnAnalsList.append(dict(
                            news_url = news_url,
                            ctprvn_cd = ctp['code'],
                            adm_sect_c = '00000',
                            register_id = user_id,
                            rgsde = 'now()',
                            updusr_id = user_id,
                            updde = 'now()'
                        ))
                        
                comptUpdateList.append(comptUpdateVO(news_url))
            
            session.bulk_insert_mappings(WssNewsAreaAnalsVO, ctprvnAnalsList)
            session.bulk_insert_mappings(WssNewsAreaAnalsVO, sggAnalsList)
            session.bulk_update_mappings(NewsColctVO, comptUpdateList)
            session.commit()
            with lock :
                offset = (threadN + threadCnt) * limit
                print("end loop : ")
                print("page : ", offset)
                
                cnt = session.query(NewsColctVO).where(NewsColctVO.area_anals_compt_at == "N").count()
                print("cnt" , cnt)
                
                record = news_rs.offset(offset).limit(limit)
        
        print("지역 분석 종료")

    except Exception as e:
        session.rollback()
        print (offset)
        print (e)
        raise e
    finally:
        close_session(session)

if __name__ == '__main__' :
    # start = time.time()
    # _test(1,1)
    for i in range(0, threadCnt) : 
        print("i, ", i)
        t = thread.Thread(target=_test, args=(i, threadCnt))
        t.start()
        
    # end = time.time()
    # print(f"{end - start:.5f} sec")