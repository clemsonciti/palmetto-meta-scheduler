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

    # This function maps the local ID with the respective job id and store the file locally
    def map_job(self, args, remoteId):

        output_file = "map_jobid.csv"

        if not os.path.isfile(output_file):
            writer = csv.writer(open(output_file, "w"))
            self.dd = 1
            writer.writerow([self.dd])
            writer.writerow(["tempJobId", "clusterName", "jobId", "scriptFiles"])
            writer.writerow([self.dd, args.to, remoteId, args.inFile])
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
                        writer.writerow([self.key, args.to, remoteId, args.inFile])
                        print(self.key)
                    break