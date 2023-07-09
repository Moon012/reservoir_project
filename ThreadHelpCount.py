import time

class ThreadHelpCount : 
    def __init__(self, n) :
        self._list = []


    def addKey(self, n) :
        self._list.append(n)
        
    def getOffsetPageKey(self, threadN) :
        self._list.index(threadN)
    
    def returnKey(self) :
        print("returnKey")
        self.count = self.count + 1
            
            