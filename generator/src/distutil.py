'''
Created on 09.03.2009

@author: dirkmeister
'''
import dist
from quantile import quantile

def load_empirical_data(filename, quantile_value = 1.00):
    data = [int(line.strip()) for line in open(filename)]
    if(quantile_value != 1.00):
        q = quantile(data, quantile_value)
        data = [d for d in data if d < q]
    return dist.EmpiricalDistribution(data)