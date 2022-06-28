from sqlalchemy import Column, PrimaryKeyConstraint, String, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class NewsKwrdYearCntVo(Base): 
    __tablename__ = 'wss_news_kwrd_year_cnt'
    
    news_year = Column(Integer, primary_key = True)
    kwrd_sn = Column(Integer, primary_key = True)
    kwrd_sm_co = Column(Integer, default = 0)
    register_id = Column(String)
    rgsde = Column(TIMESTAMP)
    updusr_id = Column(String)
    updde = Column(TIMESTAMP)