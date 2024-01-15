from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class FindMean(MRJob):
    
    # mapper function
    def mapper(self, _, line):
        r = line.split()
        if r[3] != "R2":
            yield (1, float(r[3]))
    
    # reducer function
    def reducer(self, key , value):
        total = 0
        count = 0
        for val in value:
            total = total + val
            count +=  1
        av = total/count
        yield (key , av )

if __name__ == '__main__':
    FindMean.run()