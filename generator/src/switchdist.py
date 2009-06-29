'''
Created on 12.03.2009

@author: dirkmeister
'''
import random

def load_switch_list(filename):
    class SwitchDist:
        def __init__(self, state_dist):
            self.state_dist = {}
            count = 0
            for key in state_dist:
                count = count + state_dist[key]
                self.state_dist[key] = count
            self.count = count
            
        def __repr__(self):
            return self.state_dist.__str__()
        
        def __call__(self):
            i = random.randint(0, self.count)
            for state in self.state_dist:
                if i <= self.state_dist[state]:
                    return state
            
    f = open(filename)
    d = {}
    for line in f:
        old = line[0]
        new = line[1]
        if not old in d:
            d[old] = {}
        if not new in d[old]:
            d[old][new] = 0
        d[old][new] = d[old][new] + 1
    
    result = {}
    for old_state in d:
        result[old_state] = SwitchDist(d[old_state])
    return result
    
def apply_independent_modification(result, state_tag, count):
    total_count = sum((dist.count for dist in result.values()))
    for (state, dist) in result.items():
        p = 1.0 * state.count / total_count
        delete_count = p * count
        state.state_dist["D"] = delete_count
        state.count = state.count + delete_count
    return result
    