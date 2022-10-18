from sqlalchemy import Column, String, Integer, Numeric, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class NewsKwrdCntVO(Base): 
    __tablename__ = 'wss_news_kwrd_cnt'
        
    news_url = Column(String, primary_key = True)
    kwrd_manage_no = Column(Integer)
    kwrd_colct_code = Column(String)
    kwrd_code = Column(String)
    kwrd_co = Column(String)
    register_id = Column(String)
    rgsde = Column(TIMESTAMP)
    updusr_id = Column(String)
    updde = Column(TIMESTAMP)