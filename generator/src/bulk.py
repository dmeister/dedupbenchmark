'''
Created on 17.03.2009

@author: dirkmeister
'''
class Bulk:
    def __init__(self, state, length):
        self.state = state
        self.length = length
        
    def get_data(self):
        return self.state.data.get_bulk(self.length)