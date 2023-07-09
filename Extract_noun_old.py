import re
from konlpy.tag import Komoran
import collections as ct
from threading import Lock

class Extract_noun : 
    _instance = None
    _lock = Lock()
    
    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__call__(*args, **kwargs)
        return cls._instance
    
    def __init__(self, name) :
        self.name = name
            
    # @classmethod
    # def getInstance(cls):
    #     if not cls._instance:
    #         cls._instance = Extract_noun()
    #     return cls._instance
    
    def getNouns(self, contents) :
        nouns = None
        
        if contents is not None and len(contents) > 1 :
            ko = Komoran()
            contents = re.sub('[^a-z|0-9|ㄱ-ㅎ|가-힣|\s\n]', '', contents, flags=re.I|re.M)
            nouns = ko.nouns(contents)
        
        return nouns

    def getNounsCntDict(self, nouns) :
        news_nouns_cnt_obj = None
        
        if nouns is not None and len(nouns) > 1 :
            news_nouns_cnt_obj = ct.Counter(nouns)
            news_nouns_cnt_obj = dict(news_nouns_cnt_obj)
        
        return news_nouns_cnt_obj

    