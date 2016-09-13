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

# Job class will contain all details about a given job with local and remote information.
# All elements needed to retrive all information about given job.
class Job(object):
    jobId = 0

# Type of scheduler that given resource is using e.g. Slurm, Torque,PBS Pro, Condor ...
# Commands for given resource will depend on the scheduler type. Generic class
# will support user defined commands. Specialized class available for predefined types
# of schedulers.
class Scheduler(object):
    
    # Update function: All the values of member variables are updated with the values
    # stored in the file
    def Update(self, clusterName, submitCmd, statCmd, deleteCmd, hostName, user):
        self.clusterName = clusterName
        self.submitCmd   = submitCmd
        self.statCmd     = statCmd
        self.deleteCmd   = deleteCmd
        self.hostName    = hostName
        self.user        = user
    
    # Submit function: This will take the inputs from the user and submits the
    # job on the palmetto. The list of files needed are copied from the local
    # machine to the palmetto node
    def Submit(self, args):
        path       = '/home/' + self.user
        host       = self.user + '@' + self.hostName
        splitJobScriptLocation = args.inFile.split('/')
        inputFile = splitJobScriptLocation[len(splitJobScriptLocation)-1]
        qsubCmd    = self.submitCmd + ' ' + path + '/' + inputFile
        Popen(['scp', inputFile, host + ':' + path], shell=False, stdout=PIPE)
        proc       = Popen(['ssh', host, qsubCmd], shell=False, stdout=PIPE)
        Job_.jobId = proc.communicate()[0]
        qstatCmd  = self.statCmd + ' ' + Job_.jobId
        Popen(['ssh', host, qstatCmd], shell=False, stdout=PIPE)
        if Job_.jobId != '0':
            Logger_.map_job(args)
    
    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self):
        host     = self.user + '@' + self.hostName
        qdelCmd  = self.deleteCmd + ' ' + Job_.jobId
        Popen(['ssh', host,  qdelCmd], shell=False, stdout=PIPE)

# Main class defining cluster (computing platform) resource, every element
# is representing different cluster (special handling for Open Science Grid).
class Resource(object):
    
    #Parse the configuration file and store the configuration details in the other file
    def init(self, file):
        self.ClusterName = None
        self.submitCmd   = None
        self.statCmd     = None
        self.deleteCmd   = None
        self.hostName    = None
        self.user        = None
        
        if not os.path.isfile("master_config.csv"):
            out_file = csv.writer(open("master_config.csv", "w"))
            data     = json.load(file)
            out_file.writerow(["clusterName", "submitCmd", "statCmd", "deleteCmd", "hostName", "user"])
            out_file.writerow([data['clusterName'], data['submitCmd'], data['statCmd'], data['deleteCmd'], data['hostName'], data['user']])
        else:
            out_file = csv.writer(open("master_config.csv", "a+"))
            data     = json.load(file)
            out_file.writerow([data['clusterName'], data['submitCmd'], data['statCmd'], data['deleteCmd'], data['hostName'], data['user']])

# Class responsible for logging information about jobs
class Logger(object):
    
    # This function maps the local ID with the respective job id and store the file locally
    def map_job(self, args):
        dd       = 0
        row      = 0
        key      = 0
        dir_name = args.to
        
        if not os.path.isdir(dir_name):
            os.mkdir(dir_name)
        
        os.chdir(dir_name)
        output_file = args.to + "_map_jobid.csv"

        if not os.path.isfile(output_file):
            writer = csv.writer(open(output_file, "w"))
            dd = 1
            writer.writerow([dd])
            writer.writerow(["tempJobId", "clusterName", "jobId", "scriptFiles"])
            writer.writerow([dd, dir_name, Job_.jobId, args.inFile])
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
                        writer.writerow([key, dir_name, Job_.jobId, args.inFile])
                    break
        os.chdir('..')

# Class designed to store and analyze all information about jobs run so far
class History(object):
    
    def Joblist(self, args):
        os.chdir(args.to)
        input_file = args.to + '_map_jobid.csv'
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == 'tempJobId':
                    print(row)
                    for row in reader:
                        print(row)
        os.chdir('..')

# Global configuration class
#class Config(object):

# Creating an object of the classes
Resource_ = Resource()
Schduler_ = Scheduler()
History_  = History()
Logger_   = Logger()
Job_      = Job()

# Obtaining input from the user either to submit the jobs, delete the job or
# query the job
parser = argparse.ArgumentParser(description=' A wrapper to provide commands to the scheduler to execute certain task. The first task is to initialize the'
                                             ' cluster on which the job execution should take place. A user can submit a job, delete a job, query for the status'
                                             ' of the job, get the list of all the jobs submitted'
                                             ' For example to initialize a cluster - Usage: python rsub.py initialize -h')
subparsers = parser.add_subparsers(dest='cmd')
parser_initialize = subparsers.add_parser('initialize', description=' Initialize is used to intialize the clusters. The clusters can be palmetto, OSG etc.Usage: python rsub.py initialize'
                                                                    ' <cluster_name> --configFile <confile file>')
parser_initialize.add_argument('initialize', metavar='<clusterName>', help='The name of the cluster')
parser_initialize.add_argument('--configFile', type=argparse.FileType('r'), required = True, help='The config file should be a .json file which contains configuration details to initialize the cluster')

parser_submit = subparsers.add_parser('submit', description=' A utility that allows you to submit a job to the cluster. When the user submits a job using the job script, the script will'
                                                            ' be parsed and job will be sent to the scheduler. Usage: python rsub.py submit --to <cluster_name> --inFile <input file>')
parser_submit.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')
parser_submit.add_argument('--inFile', metavar='<inputFile pbs format>', required = True, help='The script file which contains the commands which needs to be executed')

parser_delete = subparsers.add_parser('delete', description=' A utility that allows you to delete a job that was submitted using submit command.'
                                                            ' Usage: python rsub.py delete --jobId <ID of the job>')
parser_delete.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job which needs to be deleted')

parser_query = subparsers.add_parser('query', description=' A utility that provides you the status of all the jobs that has been submitted using submit command. Usage: python rsub.py'
                                                          ' query --jobId <ID of the job>')
parser_query.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job whose information is required')

parser_joblist = subparsers.add_parser('joblist', description=' A utility that provides the history of all the jobs which has been submitted on all the clusters.'
                                                              ' Usage: python rsub.py joblist --to <cluster name>')
parser_joblist.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

args = parser.parse_args()

if args.cmd == 'initialize':
    Resource_.init(args.configFile)
else:
    configFileLocation = str("master_config.csv")
    with open(configFileLocation, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['clusterName'] == args.to:
                Schduler_.Update(row['clusterName'], row['submitCmd'], row['statCmd'], row['deleteCmd'], row['hostName'], row['user'])
                break;
    if args.cmd == 'submit':
        Schduler_.Submit(args)
    elif args.cmd == 'delete':
        Schduler_.Delete()
    elif args.cmd == 'query':
        Schduler_.Query()
    elif args.cmd == 'joblist':
        History_.Joblist(args)
