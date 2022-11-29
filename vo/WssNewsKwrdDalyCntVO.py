from sqlalchemy import Column, PrimaryKeyConstraint, String, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class WssNewsKwrdDalyCntVO(Base): 
    __tablename__ = 'wss_news_kwrd_daly_cnt'
    
    news_url = Column(String, primary_key = True)
    kwrd_manage_no = Column(String, primary_key = True)
    kwrd_colct_code = Column(String, primary_key = True)
    kwrd_code = Column(String, primary_key = True)
    news_year = Column(String, primary_key = True)
    news_month = Column(String, primary_key = True)
    news_day = Column(String, primary_key = True)
    kwrd_sm_co = Column(Integer, default = 0)
    register_id = Column(String)
    rgsde = Column(TIMESTAMP)
    updusr_id = Column(String)
    updde = Column(TIMESTAMP)