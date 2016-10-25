import fileinput
import sys
import os
import os.path
import csv
import argparse
from pprint import pprint

# Class responsible for logging information about jobs
class Logger():

    def __init__(self):
        self.dd  = 0
        self.row = 0
        self.key = 0

    # This function stores the list of job objects in the file locally
    def map_job(self, args, filename):

        output_file = "map_jobid.csv"

        # Checks if the file is already present, otherwise it will create
        # a file and dump the required information in the file. If the file
        # is already present, then it will update the local ID counter inside
        # the file and maps to the current job
        if not os.path.isfile(output_file):
            writer = csv.writer(open(output_file, "w"))
            self.dd = 1
            writer.writerow([self.dd])
            writer.writerow(["tempJobId", "clusterName", "filename"])
            writer.writerow([self.dd, args.to, filename])
        else:
            with open(output_file, 'r+') as csvfile:
                reader = csv.reader(csvfile)
                for self.row in reader:
                    self.key = self.row[0]
                    self.key = int(self.key)
                    self.key += 1
                    csvfile.seek(0, 0)
                    csvfile.write(str(self.key))
                    with open(output_file, 'a+') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([self.key, args.to, filename])
                        print(self.key)
                    break