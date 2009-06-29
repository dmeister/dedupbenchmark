import random
import os
import sys
import dist
from bulk import Bulk
from data import UniqueData, RedundantData

class State:
    def __init__(self, tag, distribution, data, next):
        self.tag = tag
        self.distribution = distribution
        self.next = next
        self.data = data
        
    def get_bulk_length(self):
        return self.distribution()

class FirstGeneration:
    def __init__(self, traffic_type, redundant_bulk_length_dist, unique_bulk_length_dist):
        self.traffic_type = traffic_type
        self.redundant_bulk_length_dist = redundant_bulk_length_dist
        self.unique_bulk_length_dist = unique_bulk_length_dist
        self.states = {
                       "UNIQUE": State("U", unique_bulk_length_dist, UniqueData(), "REDUNDANT"),
                       "REDUNDANT": State("R", redundant_bulk_length_dist, RedundantData(), "UNIQUE")
                       }
        
    def print_pattern_summary(self,pattern):
        t = {"R":(0,0), "U":(0,0)}
        total = 0
        for p in pattern:
            e = t[p.state.tag]
            e = (e[0] + 1, e[1] + p.length)
            t[p.state.tag] = e
            total = total + p.length
        unique_average =    t["U"][1] / t["U"][0]
        redundant_average = t["R"][1] / t["R"][0]
        redundant_ratio = 1.0 * t["R"][1] / total 
        print "Redundant Ratio       %f" % redundant_ratio
        print "Unique    Bulk Length %f" % unique_average
        print "Redundant Bulk Length %f" % redundant_average
        
    def generate_traffic_pattern(self, bytes):
        current = bytes
        pattern = []
        
        state = self.states["UNIQUE"]
        
        while(current > 0):
            length = state.get_bulk_length()
            if(length > current):
                length = current
            pattern.append(Bulk(state, length))
            current = current - length
            state = self.states[state.next]
        return pattern
    
    def write(self, file, pattern):
        for p in pattern:
            data = p.get_data()
            file.write(data)


    