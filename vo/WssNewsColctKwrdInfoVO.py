from sqlalchemy import Column, PrimaryKeyConstraint, String, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class WssNewsColctKwrdInfoVO(Base): 
    __tablename__ = 'wss_news_colct_kwrd_info'
        
    news_url            = Column(String, primary_key = True)
    kwrd_manage_no      = Column(Integer, primary_key = True)
    kwrd_colct_code     = Column(String, primary_key = True)
    register_id         = Column(String)
    rgsde               = Column(TIMESTAMP)
    updusr_id           = Column(String)
    updde               = Column(TIMESTAMP)