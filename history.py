import fileinput
import sys
import os
import os.path
import csv
from pprint import pprint

# Class designed to store and analyze all information about jobs run so far
class History(object):

    def Joblist(self, args):
        input_file = "map_jobid.csv"
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == 'tempJobId':
                    print(row)
                    for row in reader:
                        print(row)
