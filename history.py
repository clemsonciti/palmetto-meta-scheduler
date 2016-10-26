#!/usr/bin/env python
import fileinput
import sys
import os
import os.path
import csv
import argparse
from pprint import pprint
import json
import subprocess
from subprocess import Popen, PIPE

# Obtaining input from the user to get history of the job.
# This takes the arguments from the user such as the cluster name
parser = argparse.ArgumentParser(description=' A utility that provides the history of all the jobs which has been submitted on all the clusters.'
                                                              ' Usage: python rsub.py history --to <cluster name>')

parser.add_argument('--to', metavar='<clusterName>', help='The name of the cluster on which the job has to be submitted')

# Reads the data present in the CSV file. This file contains the
# information of the local ID and the respective pickle object.
# The pickle object contains the complete information of the particular
# job.
input_file = "map_jobid.csv"
with open(input_file, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        if row[0] == 'tempJobId':
            print(row)
            for row in reader:
                 print(row)
