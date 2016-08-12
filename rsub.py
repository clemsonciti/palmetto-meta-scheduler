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

# Global variable for submit command
jobID = 0

#Class resource which stores all the information provided by the user
class Resource(object):
    __submit_cmd = None
    __stat_cmd   = None
    __delete_cmd = None
    __host_name  = None
    __user       = None
    
    #Parse the configuration file and store the configuration details in the other file
    def init(self, file):
        dir_name = args.initialize
        if not os.path.isdir(dir_name):
           os.mkdir(dir_name)
           os.chdir(dir_name)
        out_file = csv.writer(open(dir_name + ".csv", "w"))
        data = json.load(file)
        out_file.writerow(["submit_cmd", "stat_cmd", "delete_cmd", "host_name", "user"])
        out_file.writerow([data['submit_cmd'], data['stat_cmd'], data['delete_cmd'], data['host_name'], data['user']])
        os.chdir('..')
        
    # Submit function: This will take the inputs from the user and submits the
    # job on the palmetto. The list of files needed are copied from the local
    # machine to the palmetto node
    def Submit(self, inFile, __user, __host_name, __submit_cmd, __stat_cmd):
        global jobID
        path='/home/' + __user
        HOST=__user + '@' + __host_name
        CMD_QSUB=__submit_cmd + ' ' + path + '/' + inFile
        Popen(['scp', inFile, HOST + ':' + path], shell=False, stdout=PIPE)
        proc = Popen(['ssh', HOST, CMD_QSUB], shell=False, stdout=PIPE)
        jobID = proc.communicate()[0]
        CMD_QSTAT=__stat_cmd + ' ' + jobID
        Popen(['ssh', HOST, CMD_QSTAT], shell=False)

    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self, jobId, __user, __host_name, __delete_cmd):
        HOST=__user + '@' + __host_name
        CMD_QDEL=__delete_cmd + ' ' + jobId
        Popen(['ssh', HOST,  CMD_QDEL], shell=False)

    # Query function: This will take the jobID as the input from the user and
    # obtains the detailed information about the job
    def Query(self, jobId, __user, __host_name, __stat_cmd):
        HOST=__user + '@' + __host_name
        CMD_QSTAT=__stat_cmd  + ' ' + '-xf' + ' ' + jobId
        Popen(['ssh', HOST, CMD_QSTAT], shell=False)

    # Fetch function: Yet to implement
    def Fetch():
        print("Fetch!!")

# This function maps the local ID with the respective job id and store the file locally
def map_job(args):
    dd = 0
    row = 0
    key = 0
    dir_name = args.to
    os.chdir(dir_name)
    output_file = args.to + "_map_jobid.csv"

    if not os.path.isfile(output_file):
        writer = csv.writer(open(output_file, "w"))
        dd = 1
        writer.writerow([dd])
        writer.writerow(["temp_jobId", "jobId", "cluster_name"])
        if args.command == 'Submit':
            writer.writerow([dd, jobID, args.to])
        else:
            writer.writerow([dd, args.jobId, args.to])
    else:
        with open(output_file, 'r+') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                key = row[0]
                key = int(key)
                key += 1
                csvfile.seek(0, 0)
                csvfile.write(str(key))
                with open(output_file, 'a+') as csvfile:
                    writer = csv.writer(csvfile)
                    if args.command == 'Submit':
                        writer.writerow([key, jobID, args.to])
                    else:
                        writer.writerow([key, args.jobId, args.to])
                break
    os.chdir('..')

#check for arguments provided by the user. This function checks for the initialization of the cluster, Submitting
#a job, Deleting a job and querying a job
def check_arguments():
    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(1)

    #Initialize the cluster: Check for the arguments
    if args.initialize is not None:
        if not args.configFile:
            print ("Usage: Configuration file [--configFile] needs to be provided for initialization of cluster")
            sys.exit(1)
        else:
            r.init(args.configFile)

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
                r.__submit_cmd = row['submit_cmd']
                r.__stat_cmd   = row['stat_cmd']
                r.__delete_cmd = row['delete_cmd']
                r.__host_name  = row['host_name']
                r.__user       = row['user']

        #Submit command: Check for the arguments
        if args.command == 'Submit':
            if not args.inFile:
                print ("Usage: input file [--inFile] which contains the job to be submitted on the cluster")
                sys.exit(1)

            r.Submit(args.inFile, r.__user, r.__host_name, r.__submit_cmd, r.__stat_cmd)

        
        #Delete command: Check for the arguments
        if args.command == 'Delete':
            if not args.jobId:
                print ("Usage: Job ID [--jobId] needs to be provided to delete the particular job")
                sys.exit(1)
            
            r.Delete(args.jobId, r.__user, r.__host_name, r.__delete_cmd)

        #Query command: Check for the arguments
        if args.command == 'Query':
            if not args.jobId:
                print ("Usage: Job ID [--jobId] needs to be provided to query the particular job")
                sys.exit(1)
            r.Query(args.jobId, r.__user, r.__host_name, r.__stat_cmd)

        map_job(args)

#Creating an object of the class Resource
r = Resource()

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
check_arguments()
