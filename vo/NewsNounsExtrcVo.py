from sqlalchemy import Column, String, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class NewsNounsExtrcVo(Base): 
    __tablename__ = 'wss_news_nouns_extrc'
    
    news_nouns_sn = Column(Integer, primary_key = True)
    news_sn = Column(Integer)
    news_nouns = Column(String)
    news_nouns_co = Column(Integer)
    rgsde = Column(String)
    register_id = Column(TIMESTAMP)
    updusr_id = Column(String)
    upde = Column(TIMESTAMP)
        
