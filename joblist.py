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

def joblist(subparsers):
    parser_joblist = subparsers.add_parser('joblist', description=' A utility that provides the history of all the jobs which has been submitted on all the clusters.'
                                                                 ' Usage: python rsub.py joblist --to <cluster name>')

    parser_joblist.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')
