'''
Created on 18.03.2009

@author: dirkmeister
'''

class UniqueData:
    def __init__(self):
        self.random_file = open("/dev/urandom")
    def get_bulk(self, l):
        return self.random_file.read(l)
    
class RedundantData:
    def __init__(self, pattern_length = 1024 * 1024):
        self.base_pattern = open("/dev/urandom").read(pattern_length)
        self.pos = 0
    
    def get_bulk(self, l):
        data = []
        for i in xrange(l):
            data.append(self._get_next())    
        return "".join(data)
    
    def _get_next(self):
        #self.pos = 0
        self.pos = (self.pos + 1) % len(self.base_pattern)
        return self.base_pattern[self.pos]