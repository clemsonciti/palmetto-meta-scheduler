import sys
import os
import csv
from time import sleep
import subprocess
import argparse
from subprocess import Popen, PIPE

#Initialize the palmetto cluster
def palmetto(file):
   out_file = open('parse_conf.txt', 'a')
   for line in file:
      fields = line.strip().split(':')
      out_file.write(fields[1])
   out_file.close()
         	
# Submit function: This will take the inputs from the user and submits the
# job on the palmetto. The list of files needed are copied from the local
# machine to the palmetto node
def Submit(inFile):
    values = []
    conf_file = open('parse_conf.txt', 'r')
    for row in conf_file:
      for index, item in enumerate(row.split()):
         values.append(item.strip())

    #copy the values from the configuration file
    login_node = values[0]
    userName   = values[1]
    submit_cmd = values[2]
    stat_cmd   = values[4]
    path='/home/' + userName
    HOST=userName + '@' + login_node
    CMD_QSUB=submit_cmd + ' ' + path + '/' + inFile
    Popen(['scp', inFile, HOST + ':' + path], shell=False, stdout=PIPE)
    sleep(3)
    proc = Popen(['ssh', HOST, CMD_QSUB], shell=False, stdout=PIPE)
    jobID = proc.communicate()[0]
    CMD_QSTAT=stat_cmd + ' ' + jobID
    Popen(['ssh', HOST, CMD_QSTAT], shell=False)

# Delete function: This will take the jobID as the input from the user and
# deletes the particular job
def Delete(jobId):
    values = []
    conf_file = open('parse_conf.txt', 'r')
    for row in conf_file:
      for index, item in enumerate(row.split()):
         values.append(item.strip())

    #copy the values from the configuration file
    login_node = values[0]
    userName   = values[1]
    del_cmd    = values[3]
    HOST=userName + '@' + login_node
    CMD_QDEL=del_cmd + ' ' + jobId
    Popen(['ssh', HOST,  CMD_QDEL], shell=False)

# Query function: This will take the jobID as the input from the user and
# obtains the detailed information about the job
def Query(jobId):
    values = []
    conf_file = open('parse_conf.txt', 'r')
    for row in conf_file:
      for index, item in enumerate(row.split()):
         values.append(item.strip())

    #copy the values from the configuration file
    login_node = values[0]
    userName   = values[1]
    stat_cmd   = values[4]
    HOST=userName + '@' + login_node
    CMD_QSTAT=stat_cmd  + ' ' + '-xf' + ' ' + jobId
    Popen(['ssh', HOST, CMD_QSTAT], shell=False)

# Fetch function: Yet to implement
def Fetch():
    print("Fetch!!")

# Obtaining input from the user either to submit the jobs, delete the job or
# query the job
parser = argparse.ArgumentParser(description='Enter one of the following commands')
parser.add_argument('--command', action="store", choices=['Submit','Query','Delete','Fetch'])
parser.add_argument('--inFile', action="store")
parser.add_argument('--jobId', action="store")
parser.add_argument('--initialize', action="store", choices=['palmetto','stampede','osg'])
parser.add_argument('--file', type=argparse.FileType('r'),default=sys.stdin)
args = parser.parse_args()
if(args.initialize == 'palmetto'):
    palmetto(args.file)
if(args.command == 'Submit'):
    Submit(args.inFile)
elif(args.command == 'Delete'):
    Delete(args.jobId)
elif(args.command == 'Query'):
    Query(args.jobId)
else:
    exit()



