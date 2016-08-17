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
jobID    = 0

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
        writer.writerow(["temp_jobId", "jobId"])
        if args.cmd == 'submit':
            writer.writerow([dd, jobID])
        else:
            writer.writerow([dd, args.jobId])
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
                        writer.writerow([key, jobID])
                    else:
                        writer.writerow([key, args.jobId])
                break
    os.chdir('..')

#Creating an object of the class Resource
r = Resource()

# Obtaining input from the user either to submit the jobs, delete the job or
# query the job
parser = argparse.ArgumentParser(description='Enter one of the following commands')
subparsers = parser.add_subparsers(help = 'sub-command help', dest='cmd')
parser_initialize = subparsers.add_parser('initialize', help='initialize help')
parser_initialize.add_argument('initialize', action="store")
parser_initialize.add_argument('--configFile', type=argparse.FileType('r'), required = True)

parser_submit = subparsers.add_parser('submit', help='submit help')
parser_submit.add_argument('--to', action="store", required = True)
parser_submit.add_argument('--inFile', action="store", required = True)

parser_delete = subparsers.add_parser('delete', help='delete help')
parser_delete.add_argument('--to', action="store", required = True)
parser_delete.add_argument('--jobId', action="store", required = True)

parser_query = subparsers.add_parser('query', help='query help')
parser_query.add_argument('--to', action="store", required = True)
parser_query.add_argument('--jobId', action="store", required = True)

args = parser.parse_args()

if args.cmd == 'initialize':
    r.init(args.configFile)
else:
    os.chdir(args.to)
    input_file = args.to + '.csv'
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            r.__submit_cmd = row['submit_cmd']
            r.__stat_cmd   = row['stat_cmd']
            r.__delete_cmd = row['delete_cmd']
            r.__host_name  = row['host_name']
            r.__user       = row['user']
    os.chdir('..')
    if args.cmd == 'submit':
        r.Submit(args.inFile, r.__user, r.__host_name, r.__submit_cmd, r.__stat_cmd)
        map_job(args)
    elif args.cmd == 'delete':
        r.Delete(args.jobId, r.__user, r.__host_name, r.__delete_cmd)
    elif args.cmd == 'query':
        r.Query(args.jobId, r.__user, r.__host_name, r.__stat_cmd)

