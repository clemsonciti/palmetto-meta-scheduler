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
from scheduler import scheduler
from scheduler import PBS
from scheduler import Condor
from job import job
from resource import resource
from config import Config
from abstract import abstract

# Obtaining input from the user to submit the job.
# This takes the arguments from the user such as the input file,
# cluster name and submits it to respective cluster
parser = argparse.ArgumentParser(description=' A wrapper to provide commands to the scheduler to execute certain task. The first task is to initialize the'
                                             ' cluster on which the job execution should take place. A user can submit a job, delete a job, query for the status'
                                             ' of the job, get the list of all the jobs submitted'
                                             ' For example to initialize a cluster - Usage: python rsub.py initialize -h')

parser.add_argument('--inFile', metavar='<inputFile pbs format>', required = True, help='The script file which contains the commands which needs to be executed')
parser.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')
parser.add_argument('--transferType', metavar='<Method to transfer files>', help='The method in which files needs to be transferred')
parser.add_argument('--transferInpFiles', metavar='<Input files required by script file to run>', help='The extra files needed by the script file to run', nargs='+')
parser.add_argument('--transferOutFiles', metavar='<Output files obtained after submitting the job>', help='The output files generated after submitting the job', nargs='+')
args = parser.parse_args()

# Extract the arguments obtained from the user for the submit command
inputScriptFile = args.inFile
clusterName     = args.to
inpFiles        = args.transferInpFiles
outFiles        = args.transferOutFiles
fileTransferType = args.transferType

# The user will provide the data in the JSON format.
# Call the function from_json to extract the contents from JSON file
Config_ = Config()
resourceObj, subScheduler = Config_.from_json("config.json", clusterName)

Schduler_ = scheduler()

if(subScheduler == "PBS"):
    subScheduler = PBS(Schduler_)
elif(subScheduler == "Condor"):
    subScheduler = Condor(Schduler_)

# Job object is created with initializing the parameters
Job_ = job(0, 0, inputScriptFile, resourceObj.remoteTmp, inpFiles, outFiles)
output_file = "map_jobid.csv"

# Creates the pickle object for each Job. This stores the complete information
# of the Job object.
if not os.path.isfile(output_file):
    filename = 'pickle_1'
else:
    with open(output_file, 'r+') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            key = row[0]
            key = int(key)
            key += 1
            filename = 'pickle_' + str(key)
            break

subScheduler.Submit(args, Job_, filename, resourceObj)
