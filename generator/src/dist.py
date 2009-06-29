import random
import math

class Max:
    def __init__(self, max_length, root_distribution):
        self.max_length =max_length
        self.root_distribution = root_distribution
    
    def __call__(self):
        value = self.root_distribution()
        while(value > self.max_length):
            value = self.root_distribution()
        return value
    
    def raw_count(self):
        return self.root_distribution.raw_count()
    
class Additional():
    def __init__(self, value, root_distribution):
        self.value = value
        self.root_distribution = root_distribution
    
    def __call__(self):
        return self.root_distribution() + self.value
    
    def raw_count(self):
        return self.root_distribution.raw_count()

class BlockAlignment:
    def __init__(self, block_size, root_distribution):
        self.block_size = block_size
        self.root_distribution = root_distribution
    
    def __call__(self):
        chunk_size = self.root_distribution()
        if(chunk_size % self.block_size == 0):
            return chunk_size              
        return ((chunk_size / self.block_size) + 1) * self.block_size 
    
    def raw_count(self):
        return self.root_distribution.raw_count()

class ExponentialDistribution:
    def __init__(self, rate):
        self.rate = rate
        
    def __call__(self):
        return int(round(random.expovariate(self.rate)))
    
class ParetoDistribution:
    def __init__(self, shape):
        self.shape = shape
        
    def __call__(self):
        return int(round(random.paretovariate(self.shape)))
    
class EmpiricalDistribution:
    def __init__(self,data):
        self.data = sorted(data)
        self.n = len(data)-1
    
    def __call__(self):
        # k is interval
        r = random.random()
        k = int(math.floor(self.n * r))
        offset = r * self.n - (k - 1)
        y = self.data[k] * (1-offset) + self.data[k+1] * offset
        return int(y)
    
    def raw_count(self):
        return len(self.data)