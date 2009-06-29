import dist
from bulk import Bulk
from data import UniqueData, RedundantData
import switchdist

class State:
    def __init__(self, tag, distribution, data):
        self.tag = tag
        self.distribution = distribution
        self.data = data
        
    def get_bulk_length(self):
        return self.distribution()

class TemporalRedundantData:
    def __init__(self, file):
        self.file = file
    
    def get_bulk(self, l):
        return self.file.read(l)

class Generation2UniqueData:
    def __init__(self, file, unique_data):
        self.file = file
        self.unique_data = unique_data
    
    def get_bulk(self, l):
        d = self.unique_data.get_bulk(l)
        self.file.seek(l,1)
        return d

class Generation2RedundantData:
    def __init__(self, file, redundant_data):
        self.file = file
        self.redundant_data = redundant_data
    
    def get_bulk(self, l):
        d = self.redundant_data.get_bulk(l)
        self.file.seek(l,1)
        return d    

class DeletedData:
    def __init__(self, file):
        self.file = file
        
    def get_bulk(self, l):
        self.file.seek(l, 1)
        return []

class SecondGeneration:
    def __init__(self, traffic_type, old_filename, internal_dist, temporal_dist, unique_dist, switch_dist):
        self.traffic_type = traffic_type
        self.internal__dist = internal_dist
        self.temporal_dist = temporal_dist
        self.unique_dist = unique_dist
        self.old_file = open(old_filename)
        self.switch_dist = switch_dist
        if traffic_type == "block":
            self.states = {
                       "UNIQUE": State("U", unique_dist, Generation2UniqueData(self.old_file,UniqueData())),
                       "INTERNAL REDUNDANT": State("I", internal_dist, Generation2RedundantData(self.old_file,RedundantData())),
                       "TEMPORAL REDUNDANT": State("T", temporal_dist, TemporalRedundantData(self.old_file)),
                       }
        else:
                    self.states = {
                       "UNIQUE": State("U", unique_dist, UniqueData()),
                       "INTERNAL REDUNDANT": State("I", internal_dist, RedundantData()),
                       "TEMPORAL REDUNDANT": State("T", temporal_dist, TemporalRedundantData(self.old_file)),
                       }    
        self.states_by_tag = {}
        for (name, state) in self.states.items():
            self.states_by_tag[state.tag] = state
    
    def print_pattern_summary(self,pattern):
        t = {"U":(0,0), "I":(0,0), "T": (0,0)}
        total = 0
        for p in pattern:
            e = t[p.state.tag]
            e = (e[0] + 1, e[1] + p.length)
            t[p.state.tag] = e
            total = total + p.length
        temporal_redundant_average = t["T"][1] / t["T"][0]
        temporal_redundant_ratio = 1.0 * t["T"][1] / total 
        internal_redundant_average = t["I"][1] / t["I"][0]
        internal_redundant_ratio = 1.0 * t["I"][1] / total 
        unique_average = t["U"][1] / t["U"][0]
        unique_ratio = 1.0 * t["U"][1] / total 
        unqiue_count = t["U"][0]
        print "Unique Ratio                   %f" % (unique_ratio * 100)
        print "Unique Length                  %f" % unique_average
        print "Unique Count                   %f" % unqiue_count
        print "Temporal Redundant Ratio       %f" % (temporal_redundant_ratio * 100)
        print "Temporal Redundant Bulk Length %f" % temporal_redundant_average
        print "Internal Redundant Ratio       %f" % (internal_redundant_ratio * 100)
        print "Internal Redundant Bulk Length %f" % internal_redundant_average
    
    def generate_traffic_pattern(self, bytes):
        current = bytes
        pattern = []
        
        state = self.states["UNIQUE"]
        
        while(current > 0):
            length = state.get_bulk_length()
            if(length > current):
                length = current
            pattern.append(Bulk(state, length))
            if not state.tag == "D":
                current = current - length
            state = self.states_by_tag[self.switch_dist[state.tag]()]
        return pattern
    
    def get_next_state(self, state):
        dist_for_state = self.switch_dist[state.tag]
        next_state = dist_for_state()
        return next_state
    
    def write(self, file, pattern):
        for p in pattern:
            data = p.get_data()
            file.write(data)