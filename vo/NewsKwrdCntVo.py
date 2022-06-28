from sqlalchemy import Column, String, Integer, Numeric, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class NewsKwrdCntVo(Base): 
    __tablename__ = 'wss_news_kwrd_cnt'
    
    news_sn = Column(Integer, primary_key = True)
    kwrd_sn = Column(Integer)
    kwrd_year = Column(Integer)
    kwrd_co = Column(Integer)
    register_id = Column(String)
    rgsde = Column(TIMESTAMP)
    updusr_id = Column(String)
    updde = Column(TIMESTAMP)
