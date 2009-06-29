import random
import os
import sys
import dist
from traffic_first_gen import FirstGeneration
from traffic_second_gen import SecondGeneration
from distutil import load_empirical_data
from quantile import quantile
import switchdist

def calculate_unique_mean(redundant_mean, redundant_ratio):
    """ Calculates the average bulk length mean of unique data using the redundant mean, the (inner)
        redundancy ratio under the assumption of an exponential distribution"""
    return (redundant_mean / redundant_ratio) - redundant_mean

def print_pattern(pattern):
    for p in pattern:
        l = p.length / 1024
        for i in xrange(l):
            print p.state.tag,

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print "Illegal arguments"
        sys.exit()
        
    bytes = 1024 * 1024 * 1024 * 2
    type = sys.argv[1]
    if(type == "first"):
        gen1_filename = sys.argv[2]    
        gen1 = FirstGeneration("block",dist.BlockAlignment(4 * 1024, dist.Additional(4 * 1024, load_empirical_data("bulklengthstats/upb-6-rabin8-redundant-bulk-list.csv", 0.999))),
                               dist.BlockAlignment(4 * 1024, load_empirical_data("bulklengthstats/upb-6-rabin8-unique-bulk-list.csv", 0.999)))
        pattern = gen1.generate_traffic_pattern(bytes)
        gen1.print_pattern_summary(pattern)
        f = open(gen1_filename, "w")
        gen1.write(f, pattern)
        f.close()
    elif type == "second":
        gen1_filename = sys.argv[2]
        gen2_filename = sys.argv[3]
        gen2 = SecondGeneration("block",gen1_filename, 
                                dist.BlockAlignment(4 * 1024, load_empirical_data("bulklengthstats/upb-6-rabin8-internal-redundant-bulk-list.csv",0.999)),
                                dist.BlockAlignment(4 * 1024, load_empirical_data("bulklengthstats/upb-6-rabin8-temporal-redundant-bulk-list.csv",0.999)),
                                dist.BlockAlignment(4 * 1024, load_empirical_data("bulklengthstats/upb-6-rabin8-temporal-unique-bulk-list.csv",0.999)),
                                switchdist.load_switch_list("bulklengthstats/upb-6-rabin8-switch-stats.csv")
        #internal_dist, temporal_dist, unique_dist, switch_dist
                            )
        pattern = gen2.generate_traffic_pattern(bytes)
        gen2.print_pattern_summary(pattern)
        f = open(gen2_filename, "w")
        gen2.write(f, pattern)
        f.close()
    elif(type == "floating-first"):
        gen1_filename = sys.argv[2]    
        gen1 = FirstGeneration("floating",dist.Additional(4 * 1024, load_empirical_data("bulklengthstats/upb-20-s-rabin8-redundant-bulk-list.csv", 0.999)),
                               load_empirical_data("bulklengthstats/upb-20-s-rabin8-unique-bulk-list.csv", 0.999))
        pattern = gen1.generate_traffic_pattern(bytes)
        gen1.print_pattern_summary(pattern)
        f = open(gen1_filename, "w")
        gen1.write(f, pattern)
        f.close()
    elif type == "floating-second":
        gen1_filename = sys.argv[2]
        gen2_filename = sys.argv[3]
        gen2 = SecondGeneration("floating",gen1_filename, 
                                load_empirical_data("bulklengthstats/upb-21-s-rabin8-internal-redundant-bulk-list.csv",0.999),
                                load_empirical_data("bulklengthstats/upb-21-s-rabin8-temporal-redundant-bulk-list.csv",0.999),
                                load_empirical_data("bulklengthstats/upb-21-s-rabin8-temporal-unique-bulk-list.csv",0.999),
                                switchdist.load_switch_list("bulklengthstats/upb-21-s-rabin8-switch-stats.csv")
        #internal_dist, temporal_dist, unique_dist, switch_dist
                            )
        pattern = gen2.generate_traffic_pattern(bytes)
        gen2.print_pattern_summary(pattern)
        f = open(gen2_filename, "w")
        gen2.write(f, pattern)
        f.close()
    