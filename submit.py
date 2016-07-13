import sys
import os
from time import sleep
import subprocess
import argparse
from subprocess import Popen, PIPE

# Submit function: This will take the inputs from the user and submits the
# job on the palmetto. The list of files needed are copied from the local
# machine to the palmetto node
def Submit(userName, inFile, folderName):
    path='/home/' + userName + '/' + folderName
    HOST=userName + '@user.palmetto.clemson.edu'
    CMD_CREATEFOLD='mkdir' + ' ' + folderName
    CMD_QSUB='qsub' + ' ' + path + '/' + inFile
    Popen(['ssh', HOST, CMD_CREATEFOLD], shell=False, stdout=PIPE)
    Popen(['scp', inFile, HOST + ':' + path], shell=False, stdout=PIPE)
    sleep(3)
    proc = Popen(['ssh', HOST, CMD_QSUB], shell=False, stdout=PIPE)
    jobID = proc.communicate()[0]
    CMD_QSTAT='qstat' + ' ' + jobID
    Popen(['ssh', HOST, CMD_QSTAT], shell=False)

# Delete function: This will take the jobID as the input from the user and
# deletes the particular job
def Delete(userName, jobId):
    HOST=userName + '@user.palmetto.clemson.edu'
    CMD_QDEL='qdel' + ' ' + jobId
    Popen(['ssh', HOST,  CMD_QDEL], shell=False)

# Query function: This will take the jobID as the input from the user and
# obtains the detailed information about the job
def Query(userName, jobId):
    HOST=userName + '@user.palmetto.clemson.edu'
    CMD_QSTAT='qstat' + ' ' + '-xf' + ' ' + jobId
    Popen(['ssh', HOST, CMD_QSTAT], shell=False)

# Fetch function: Yet to implement
def Fetch():
    print("Fetch!!")

# Obtaining input from the user either to submit the jobs, delete the job or
# query the job
parser = argparse.ArgumentParser(description='Enter one of the following commands')
parser.add_argument('--command', action="store", choices=['Submit','Query','Delete','Fetch'])
parser.add_argument('--folderName', action="store")
parser.add_argument('--inFile', action="store")
parser.add_argument('--userName', action="store")
parser.add_argument('--jobId', action="store")
args = parser.parse_args()
if(args.command == 'Submit'):
    Submit(args.userName, args.inFile, args.folderName)
elif(args.command == 'Delete'):
    Delete(args.userName, args.jobId)
elif(args.command == 'Query'):
    Query(args.userName, args.jobId)
else:
    exit()



