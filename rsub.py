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
    jobID = 0

# Type of scheduler that given resource is using e.g. Slurm, Torque,PBS Pro, Condor ...
# Commands for given resource will depend on the scheduler type. Generic class
# will support user defined commands. Specialized class available for predefined types
# of schedulers.
class Scheduler(object):
    
    # Update function: All the values of member variables are updated with the values
    # stored in the file
    def Update(self, submit_cmd, stat_cmd, delete_cmd, host_name, user):
        self.submit_cmd = submit_cmd
        self.stat_cmd   = stat_cmd
        self.delete_cmd = delete_cmd
        self.host_name  = host_name
        self.user       = user
    
    # Submit function: This will take the inputs from the user and submits the
    # job on the palmetto. The list of files needed are copied from the local
    # machine to the palmetto node
    def Submit(self, args):
        path       = '/home/' + self.user
        HOST       = self.user + '@' + self.host_name
        CMD_QSUB   = self.submit_cmd + ' ' + path + '/' + args.inFile
        Popen(['scp', args.inFile, HOST + ':' + path], shell=False, stdout=PIPE)
        proc       = Popen(['ssh', HOST, CMD_QSUB], shell=False, stdout=PIPE)
        Job_.jobID = proc.communicate()[0]
        CMD_QSTAT  = self.stat_cmd + ' ' + Job_.jobID
        Popen(['ssh', HOST, CMD_QSTAT], shell=False, stdout=PIPE)
        Logger_.map_job(args)
    
    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self):
        HOST     = self.user + '@' + self.host_name
        CMD_QDEL = self.delete_cmd + ' ' + Job_.jobID
        Popen(['ssh', HOST,  CMD_QDEL], shell=False, stdout=PIPE)

# Main class defining cluster (computing platform) resource, every element
# is representing different cluster (special handling for Open Science Grid).
class Resource(object):
    
    #Scheduler_ = Scheduler()
    
    #Parse the configuration file and store the configuration details in the other file
    def init(self, file):
        self.submit_cmd = None
        self.stat_cmd   = None
        self.delete_cmd = None
        self.host_name  = None
        self.user       = None
        
        out_file = csv.writer(open("master_config.csv", "a+"))
        data     = json.load(file)
        out_file.writerow(["submit_cmd", "stat_cmd", "delete_cmd", "host_name", "user"])
        out_file.writerow([data['submit_cmd'], data['stat_cmd'], data['delete_cmd'], data['host_name'], data['user']])

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
            writer.writerow(["temp_jobId", "cluster_name", "jobId"])
            writer.writerow([dd, dir_name, Job_.jobID])
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
                        writer.writerow([key, dir_name, Job_.jobID])
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
                if row[0] == 'temp_jobId':
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
parser = argparse.ArgumentParser(description='Enter one of the following commands')
subparsers = parser.add_subparsers(help = 'sub-command help', dest='cmd')
parser_initialize = subparsers.add_parser('initialize', description='Usage: python rsub.py initialize <cluster_name> --configFile <confile file>')
parser_initialize.add_argument('initialize', action="store", help='The name of the cluster')
parser_initialize.add_argument('--configFile', type=argparse.FileType('r'), required = True, help='The config file should be a .json file which contains configuration details to initialize the cluster')

parser_submit = subparsers.add_parser('submit', description='Usage: python rsub.py submit --to <cluster_name> --inFile <input file>')
parser_submit.add_argument('--to', action="store", required = True, help='The name of the cluster on which the job has to be submitted')
parser_submit.add_argument('--inFile', action="store", required = True, help='The script file which contains the commands which needs to be executed')

parser_delete = subparsers.add_parser('delete', description='Usage: python rsub.py delete --jobId <ID of the job>')
parser_delete.add_argument('--jobId', action="store", required = True, help='The ID of the job which needs to be deleted')

parser_query = subparsers.add_parser('query', description='Usage: python rsub.py query --jobId <ID of the job>')
parser_query.add_argument('--jobId', action="store", required = True, help='The ID of the job whose information is required')

parser_joblist = subparsers.add_parser('joblist', description='Usage: python rsub.py joblist --to <cluster name>')
parser_joblist.add_argument('--to', action="store", required = True, help='The name of the cluster on which the job has to be submitted')

args = parser.parse_args()

if args.cmd == 'initialize':
    r.init(args.configFile)
else:
    input_file = "master_config.csv"
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            Schduler_.Update(row['submit_cmd'], row['stat_cmd'], row['delete_cmd'], row['host_name'], row['user'])
    if args.cmd == 'submit':
       Schduler_.Submit(args)
    elif args.cmd == 'delete':
        Schduler_.Delete()
    elif args.cmd == 'query':
        Schduler_.Query()
    elif args.cmd == 'joblist':
        History_.Joblist(args)
