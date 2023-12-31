from sqlalchemy import Column, String, Integer, Date, TIMESTAMP, Text
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class NewsColctVO(Base):
    __tablename__ = 'wss_news_colct'
    
    news_sn = Column(Integer)
    news_nsprc = Column(Text)
    news_wrter = Column(Text)
    news_sj = Column(Text)
    news_bdt = Column(Text)
    news_url = Column(Text, primary_key = True)
    news_rgsde = Column(TIMESTAMP)
    news_updde = Column(TIMESTAMP)
    rgsde = Column(TIMESTAMP)
    updde = Column(TIMESTAMP)
    news_cl_code = Column(Text)
    news_dc_code = Column(Text)
    news_noun = Column(Text)
    ndls_wrd = Column(Text)
    area_anals_compt_at = Column(Text)
    ndls_wrd_anals_compt_at = Column(Text)
    noun_anals_compt_at = Column(Text)