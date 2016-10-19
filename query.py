import fileinput
import sys
import os
import os.path
import json
import csv
import pickle
from time import sleep
import subprocess
import argparse
from subprocess import Popen, PIPE
from pprint import pprint

def parseQuery(subparsers):
    parser_query = subparsers.add_parser('query', description=' A utility that provides you the status of all the jobs that has been submitted using submit command. Usage: python rsub.py'
                                                            ' query --jobId <ID of the job>')

    parser_query.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job whose information is required')
    parser_query.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

def query(schduler, resource, args):
    with open('data') as f1:
        Job_ = pickle.load(f1)
        print(Job_)
        schduler.Query(args, Job_, resource)