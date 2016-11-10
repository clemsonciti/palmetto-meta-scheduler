#!/usr/bin/env python
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
from config import Config
from scheduler import scheduler
from scheduler import PBS
from scheduler import Condor

# Obtaining input from the user to query the job.
# This takes the arguments from the user such as the job id and cluster name
parser = argparse.ArgumentParser(description=' A utility that provides you the status of all the jobs that has been submitted using submit command. Usage: python rsub.py'
                                             ' query --jobId <ID of the job>')

parser.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job whose information is required')
parser.add_argument('--to', metavar='<clusterName>', help='The name of the cluster on which the job has to be submitted')

# Extract the arguments obtained from the user for the delete command
args = parser.parse_args()
jobId = args.jobId

if args.to is not None:
    clusterName = args.to
else:
    clusterName = None

# The user will provide the data in the JSON format.
# Call the function from_json to extract the contents from JSON file
Config_ = Config()
resourceObj, subScheduler = Config_.from_json("config.json", clusterName)

Schduler_ = scheduler()

if(subScheduler == "PBS"):
    subScheduler = PBS(Schduler_)
elif(subScheduler == "Condor"):
    subScheduler = Condor(Schduler_)

# Reads the information of the particular job object. This is
# done based on the jobId provided by the user
inp_file = 'pickle_' + jobId
with open(inp_file, 'rb') as f:
    Job_ = pickle.load(f)

subScheduler.Query(args, Job_, resourceObj)