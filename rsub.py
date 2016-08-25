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
#jobID    = 0

#class Job(object):
#jobID = 0

#Class resource which stores all the information provided by the user
class Resource(object):
    
    #Parse the configuration file and store the configuration details in the other file
    def init(self, file):
        self.submit_cmd = None
        self.stat_cmd   = None
        self.delete_cmd = None
        self.host_name  = None
        self.user       = None
        
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
    def Submit(self, inFile):
        global jobID
        path='/home/' + self.user
        HOST=self.user + '@' + self.host_name
        CMD_QSUB=self.submit_cmd + ' ' + path + '/' + inFile
        Popen(['scp', inFile, HOST + ':' + path], shell=False, stdout=PIPE)
        proc = Popen(['ssh', HOST, CMD_QSUB], shell=False, stdout=PIPE)
        jobID = proc.communicate()[0]
        CMD_QSTAT=self.stat_cmd + ' ' + jobID
        Popen(['ssh', HOST, CMD_QSTAT], shell=False, stdout=PIPE)

    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self, jobId):
        HOST=self.user + '@' + self.host_name
        CMD_QDEL=self.delete_cmd + ' ' + jobId
        Popen(['ssh', HOST,  CMD_QDEL], shell=False, stdout=PIPE)

    # Query function: This will take the jobID as the input from the user and
    # obtains the detailed information about the job
    def Query(self, jobId):
        HOST=self.user + '@' + self.host_name
        CMD_QSTAT=self.stat_cmd  + ' ' + '-xf' + ' ' + jobId
        Popen(['ssh', HOST, CMD_QSTAT], shell=False, stdout=PIPE)
    
    # Fetch function: Yet to implement
    def Fetch():
        print("Fetch!!")
    
    def Update(self, submit_cmd, stat_cmd, delete_cmd, host_name, user):
        self.submit_cmd = submit_cmd
        self.stat_cmd = stat_cmd
        self.delete_cmd = delete_cmd
        self.host_name = host_name
        self.user = user
    
    def joblist(self, args):
       os.chdir(args.to)
       input_file = args.to + '_map_jobid.csv'
       with open(input_file, 'r') as f:
          reader = csv.reader(f)
          for row in reader:
             if row[0] == 'temp_jobId':
                 print(row)
                 for row in reader:
                     print(row)

# This function maps the local ID with the respective job id and store the file locally
def map_job(args):
    dd  = 0
    row = 0
    key = 0
    dir_name = args.to
    os.chdir(dir_name)
    output_file = args.to + "_map_jobid.csv"

    if not os.path.isfile(output_file):
        writer = csv.writer(open(output_file, "w"))
        dd = 1
        writer.writerow([dd])
        writer.writerow(["temp_jobId", "cluster_name", "jobId"])
        if args.cmd == 'submit':
            writer.writerow([dd, args.to, jobID])
        else:
            writer.writerow([dd, args.to, args.jobId])
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
                    if args.cmd == 'submit':
                        writer.writerow([key, args.to, jobID])
                    else:
                        writer.writerow([key, args.to, args.jobId])
                break
    os.chdir('..')

#Creating an object of the class Resource
r = Resource()

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
    os.chdir(args.to)
    input_file = args.to + '.csv'
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            r.Update(row['submit_cmd'], row['stat_cmd'], row['delete_cmd'], row['host_name'], row['user'])
    os.chdir('..')
    if args.cmd == 'submit':
        r.Submit(args.inFile)
        map_job(args)
    elif args.cmd == 'delete':
        r.Delete(args.jobId)
    elif args.cmd == 'query':
        r.Query(args.jobId)
    elif args.cmd == 'joblist':
        r.joblist(args)

