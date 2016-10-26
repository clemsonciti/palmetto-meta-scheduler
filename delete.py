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

# Obtaining input from the user to delete the job.
# This takes the arguments from the user such as the job id and cluster name
parser = argparse.ArgumentParser(description=' A utility that allows you to delete a job that was submitted using submit command.'
                                             ' Usage: python rsub.py delete --jobId <ID of the job>')

parser.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job which needs to be deleted')
parser.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')
parser.add_argument('--transferType', metavar='<Method to transfer files>', help='The method in which files needs to be transferred')

# Extract the arguments obtained from the user for the delete command
args = parser.parse_args()
jobId = args.jobId
clusterName = args.to

# The user will provide the data in the JSON format.
# Call the function from_json to extract the contents from JSON file
Config_ = Config()
resourceObj, subScheduler = Config_.from_json("config.json", clusterName)

Schduler_ = scheduler()

# Creating schedulers based on the input provided by the user
if(subScheduler == "PBS"):
    subScheduler = PBS(Schduler_)
elif(subScheduler == "Condor"):
    subScheduler = Condor(Schduler_)

# Reads the information of the particular job object. This is
# done based on the jobId provided by the user
inp_file = 'pickle_' + jobId
with open(inp_file, 'rb') as f:
    Job_ = pickle.load(f)
subScheduler.Delete(args, Job_, resourceObj)