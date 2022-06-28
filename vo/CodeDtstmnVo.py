from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class CodeDtstmnVo(Base): 
    __tablename__ = 'wss_code_dtstmn'
    
    code_sn = Column(Integer, primary_key = True)
    code_column_nm = Column(String)
    code_no = Column(String)
    code_dc = Column(String)
    code_usgstt = Column(String)
