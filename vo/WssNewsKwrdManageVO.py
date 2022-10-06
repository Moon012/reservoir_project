from sqlalchemy import Column, PrimaryKeyConstraint, String, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class WssNewsKwrdManageVO(Base): 
    __tablename__ = 'wss_news_kwrd_manage'
        
    kwrd_manage_no      = Column(Integer, primary_key = True, autoincrement=True)
    kwrd_manage_nm      = Column(String)
    kwrd_manage_cn      = Column(String)
    kwrd_manage_bgnde   = Column(TIMESTAMP)
    kwrd_manage_endde   = Column(TIMESTAMP)
    use_yn              = Column(String)
    delete_yn           = Column(String)
    frst_register_id    = Column(String)
    frst_regist_pnttm   = Column(TIMESTAMP)
    last_updusr_id      = Column(String)
    last_updt_pnttm     = Column(TIMESTAMP)
