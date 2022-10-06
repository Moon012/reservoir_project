from sqlalchemy import Column, String, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class NewsNounsExtrcVO(Base): 
    __tablename__ = 'wss_news_nouns_extrc'
    news_url
    news_nouns
    news_nouns_co
    register_id
    rgsde
    updusr_id
    updde

    kwrd_manage_no      = Column(Integer, primary_key = True)
    kwrd_colct_code     = Column(String, primary_key = True)
    kwrd_code           = Column(String, primary_key = True)
    kwrd_nm             = Column(String)
    kwrd_level          = Column(Integer)
    sort_sn             = Column(Integer)
    use_yn              = Column(String)
    delete_yn           = Column(String)
    frst_register_id    = Column(String)
    frst_regist_pnttm   = Column(TIMESTAMP)
    last_updusr_id      = Column(String)
    last_updt_pnttm     = Column(TIMESTAMP)
        
