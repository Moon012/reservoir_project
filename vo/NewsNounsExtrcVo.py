from sqlalchemy import Column, String, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class NewsNounsExtrcVO(Base): 
    __tablename__ = 'wss_news_nouns_extrc'
    news_url            = Column(String, primary_key = True)
    news_nouns          = Column(String, primary_key = True)
    news_nouns_co       = Column(Integer)
    register_id         = Column(String)
    rgsde               = Column(TIMESTAMP)
    updusr_id           = Column(String)
    updde               = Column(TIMESTAMP)