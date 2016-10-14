import fileinput
import sys
import os
import os.path
import json
import csv
from time import sleep
import subprocess
import argparse
from subprocess import Popen, PIPE
from pprint import pprint
#import rsub

def query(subparsers):
    parser_query = subparsers.add_parser('query', description=' A utility that provides you the status of all the jobs that has been submitted using submit command. Usage: python rsub.py'
                                                            ' query --jobId <ID of the job>')

    parser_query.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job whose information is required')
    parser_query.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')
