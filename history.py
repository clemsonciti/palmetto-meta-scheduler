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


def parseHistory(subparsers):
    parser_history = subparsers.add_parser('history', description=' A utility that provides the history of all the jobs which has been submitted on all the clusters.'
                                                                 ' Usage: python rsub.py history --to <cluster name>')

    parser_history.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

def history(args):
    input_file = "map_jobid.csv"
    with open(input_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0] == 'tempJobId':
                print(row)
                for row in reader:
                    print(row)
