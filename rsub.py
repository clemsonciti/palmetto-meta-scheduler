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

#Parse the configuration file
def parse_config(file):
    out_file = csv.writer(open("parse_conf.csv", "w"))
    data = json.load(file)
    out_file.writerow(["submit_cmd", "stat_cmd", "delete_cmd", "host_name", "user"])
    out_file.writerow([data['submit_cmd'], data['stat_cmd'], data['delete_cmd'], data['host_name'], data['user']])

#Class resource which stores all the information provided by the user
class Resource:
   __submit_cmd = None
   __stat_cmd   = None
   __delete_cmd = None
   __host_name  = None
   __user       = None

# Submit function: This will take the inputs from the user and submits the
# job on the palmetto. The list of files needed are copied from the local
# machine to the palmetto node
def Submit(inFile):
    path='/home/' + __user
    HOST=__user + '@' + __host_name
    CMD_QSUB=__submit_cmd + ' ' + path + '/' + inFile
    Popen(['scp', inFile, HOST + ':' + path], shell=False, stdout=PIPE)
    sleep(3)
    proc = Popen(['ssh', HOST, CMD_QSUB], shell=False, stdout=PIPE)
    jobID = proc.communicate()[0]
    CMD_QSTAT=__stat_cmd + ' ' + jobID
    Popen(['ssh', HOST, CMD_QSTAT], shell=False)

# Delete function: This will take the jobID as the input from the user and
# deletes the particular job
def Delete(jobId):
    HOST=__user + '@' + __host_name
    CMD_QDEL=__delete_cmd + ' ' + jobId
    Popen(['ssh', HOST,  CMD_QDEL], shell=False)

# Query function: This will take the jobID as the input from the user and
# obtains the detailed information about the job
def Query(jobId):
    HOST=__user + '@' + __host_name
    CMD_QSTAT=__stat_cmd  + ' ' + '-xf' + ' ' + jobId
    Popen(['ssh', HOST, CMD_QSTAT], shell=False)

# Fetch function: Yet to implement
def Fetch():
    print("Fetch!!")

# Obtaining input from the user either to submit the jobs, delete the job or
# query the job
parser = argparse.ArgumentParser(description='Enter one of the following commands')
parser.add_argument('--initialize', action="store", choices=['palmetto','stampede','osg'])
parser.add_argument('--configFile', type=argparse.FileType('r'))
parser.add_argument('--command', action="store", choices=['Submit','Query','Delete','Fetch'])
parser.add_argument('--to', action="store", choices=['palmetto','stampede','osg'])
parser.add_argument('--inFile', action="store")
parser.add_argument('--jobId', action="store")
args = parser.parse_args()

#check for arguments if no arguments is provided then print help
if len(sys.argv) < 2:
   parser.print_usage()
   sys.exit(1)

#Initialize the cluster: Check for the arguments
if args.initialize is not None:
   if not args.configFile:
      print ("Usage: Configuration file [--configFile] needs to be provided for initialization of cluster")
      sys.exit(1)
   else:
      parse_config(args.configFile)

#Command: Check for the arguments
if args.command is not None:
    if not args.to:
       print ("Usage: to [--to] needs to be provided as to which cluster the command needs to be submitted")
       sys.exit(1)

if not os.path.isfile('parse_conf.csv'):
   print ("Usage: [--initialize] Cluster is not initialized before")
   sys.exit(1)

with open('parse_conf.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        __submit_cmd = row['submit_cmd']
        __stat_cmd   = row['stat_cmd']
        __delete_cmd = row['delete_cmd']
        __host_name  = row['host_name']
        __user       = row['user']
    
    #Submit command: Check for the arguments
    if args.command == 'Submit':
        if not args.inFile:
            print ("Usage: input file [--inFile] which contains the job to be submitted on the cluster")
            sys.exit(1)
        Submit(args.inFile)

    #Delete command: Check for the arguments
    if args.command == 'Delete':
        if not args.jobId:
            print ("Usage: Job ID [--jobId] needs to be provided to delete the particular job")
            sys.exit(1)
        Delete(args.jobId)

    #Query command: Check for the arguments
    if args.command == 'Query':
        if not args.jobId:
            print ("Usage: Job ID [--jobId] needs to be provided to query the particular job")
            sys.exit(1)
        Query(args.jobId)
