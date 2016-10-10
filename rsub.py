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

class job(object):
    def __init__(self):
        self.jobId = 1
        self.JobName = "aa"
        self.dest = "bb"

    def translateScript(self):
        print("translation")

    def jobFetch(self):
        print("Fetch")

    def transferInpFile(self):
        print("input files transfer")

    def transferOutFiles(self):
        print("output files transfer")

class scheduler(object):
    def __init__(self, submitCmd, statCmd, deleteCmd, hostName, user, remoteId):
        self.submitCmd = submitCmd
        self.statCmd   = statCmd
        self.deleteCmd = deleteCmd
        self.hostName  = hostName
        self.user      = user
        self.remoteId  = remoteId

    # Submit function: This will take the inputs from the user and submits the
    # job on the palmetto. The list of files needed are copied from the local
    # machine to the palmetto node
    def Submit(self, args, Job_):
        path = '/home/' + self.user
        host = self.user + '@' + self.hostName
        splitJobScriptLocation = args.inFile.split('/')
        inputFile = splitJobScriptLocation[len(splitJobScriptLocation) - 1]
        qsubCmd = self.submitCmd + ' ' + path + '/' + inputFile
        Popen(['scp', inputFile, host + ':' + path], shell=False)
        sleep(1)
        self.remoteId = subprocess.Popen(['ssh', host, qsubCmd], shell=False, stdout=subprocess.PIPE)

    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self, args):
        input_file = 'map_jobid.csv'
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[0] == args.jobId and row[1] == args.to:
                    self.remoteId = row[2]
                    return

            print("Invalid jobId" + ' ' + args.jobId + ' ' + "to" + ' ' + args.to)
            sys.exit()

    def Query(self, args):
        input_file = 'map_jobid.csv'
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[0] == args.jobId:
                    self.remoteId = row[2]
                    break

    def from_json(self, config_json, args):
        with open(config_json, 'r') as f:
            params = json.load(f)
            if(args.to == "palmetto"):
                PBS_schduler_ = PBS(Schduler_)
                PBS_schduler_.user = params["Scheduler"][0]["userName"]
                PBS_schduler_.hostName = params["Scheduler"][0]["hostName"]
                if args.cmd == 'submit':
                    Job_ = job()
                    PBS_schduler_.Submit(args, Job_)
                elif args.cmd == 'delete':
                    PBS_schduler_.Delete(args)
                elif args.cmd == 'query':
                    PBS_schduler_.Query(args)
                elif args.cmd == 'joblist':
                    History_ = History()
                    History_.Joblist(args)
            elif(args.to == "OSG"):
                Condor_schduler_ = Condor(Schduler_)
                Condor_schduler_.user = params["Scheduler"][1]["userName"]
                Condor_schduler_.hostName = params["Scheduler"][1]["hostName"]
                if args.cmd == 'submit':
                    Condor_schduler_.Submit(args)
                elif args.cmd == 'delete':
                    Condor_schduler_.Delete(args)
                elif args.cmd == 'query':
                    Condor_schduler_.Query(args)
                elif args.cmd == 'joblist':
                    History_ = History()
                    History_.Joblist(args)

class PBS(scheduler):

    def __init__(self, scheduler):
        self.submitCmd = "qsub"
        self.statCmd   = "qstat -xf"
        self.deleteCmd = "qdel"

    def Submit(self, args, Job_):
        super(PBS, self).Submit(args, Job_)
        Logger_ = Logger()
        self.remoteId = self.remoteId.communicate()[0]
        if self.remoteId != '0':
            Logger_.map_job(args, self.remoteId, Job_)

    def Delete(self, args):
        super(PBS, self).Delete(args)
        host = Schduler_.user + '@' + Schduler_.hostName
        print(host)
        qdelCmd = self.deleteCmd + ' ' + self.remoteId
        Popen(['ssh', host, qdelCmd], shell=False, stdout=PIPE)

    def Query(self, args):
        super(PBS, self).Query(args)
        host = self.user + '@' + self.hostName
        qstatCmd = self.statCmd + ' ' + '-xf' + ' ' + self.remoteId
        Popen(['ssh', host, qstatCmd], shell=False)


class Condor(scheduler):
    def __init__(self, scheduler):
        self.submitCmd = "condor_submit"
        self.statCmd = "condor_q"
        self.deleteCmd = "condor_rm"

    def Submit(self, args):
        Logger_ = Logger()
        super(Condor, self).Submit(args)
        for line in self.remoteId.stdout:
            if "cluster" in line:
                self.remoteId = line.split("cluster", 1)[1]
                if self.remoteId != '0':
                    Logger_.map_job(args, self.remoteId)

    def Delete(self, args):
        super(Condor, self).Delete(args)
        host = self.user + '@' + self.hostName
        qdelCmd = self.deleteCmd + ' ' + self.remoteId
        print(self.deleteCmd)
        Popen(['ssh', host, qdelCmd], shell=False, stdout=PIPE)
        os.chdir('..')

    def Query(self, args):
        super(Condor, self).Query(args)
        host = self.user + '@' + self.hostName
        qstatCmd = self.statCmd + ' ' + self.remoteId
        Popen(['ssh', host, qstatCmd], shell=False)
        os.chdir('..')

# Class responsible for logging information about jobs
class Logger():

    def __init__(self):
        self.dd  = 0
        self.row = 0
        self.key = 0

    # This function maps the local ID with the respective job id and store the file locally
    def map_job(self, args, remoteId, Job_):

        output_file = "map_jobid.csv"

        if not os.path.isfile(output_file):
            writer = csv.writer(open(output_file, "w"))
            self.dd = 1
            writer.writerow([self.dd])
            writer.writerow(["tempJobId", "clusterName", "jobId", "scriptFiles"])
            writer.writerow([self.dd, args.to, remoteId, args.inFile])
        else:
            with open(output_file, 'r+') as csvfile:
                reader = csv.reader(csvfile)
                for self.row in reader:
                    self.key = self.row[0]
                    self.key = int(self.key)
                    self.key += 1
                    csvfile.seek(0, 0)
                    csvfile.write(str(self.key))
                    with open(output_file, 'a+') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([self.key, args.to, remoteId, args.inFile])
                        print(self.key)
                    break

# Class designed to store and analyze all information about jobs run so far
class History(object):

    def Joblist(self, args):
        input_file = "map_jobid.csv"
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == 'tempJobId':
                    print(row)
                    for row in reader:
                        print(row)

# Obtaining input from the user either to submit the jobs, delete the job or
# query the job
parser = argparse.ArgumentParser(description=' A wrapper to provide commands to the scheduler to execute certain task. The first task is to initialize the'
                                             ' cluster on which the job execution should take place. A user can submit a job, delete a job, query for the status'
                                             ' of the job, get the list of all the jobs submitted'
                                             ' For example to initialize a cluster - Usage: python rsub.py initialize -h')
subparsers = parser.add_subparsers(dest='cmd')

parser_submit = subparsers.add_parser('submit', description=' A utility that allows you to submit a job to the cluster. When the user submits a job using the job script, the script will'
                                                            ' be parsed and job will be sent to the scheduler. Usage: python rsub.py submit --to <cluster_name> --inFile <input file>')
parser_submit.add_argument('--inFile', metavar='<inputFile pbs format>', required = True, help='The script file which contains the commands which needs to be executed')
parser_submit.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

parser_delete = subparsers.add_parser('delete', description=' A utility that allows you to delete a job that was submitted using submit command.'
                                                            ' Usage: python rsub.py delete --jobId <ID of the job>')
parser_delete.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job which needs to be deleted')
parser_delete.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

parser_query = subparsers.add_parser('query', description=' A utility that provides you the status of all the jobs that has been submitted using submit command. Usage: python rsub.py'
                                                          ' query --jobId <ID of the job>')
parser_query.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job whose information is required')
parser_query.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

parser_joblist = subparsers.add_parser('joblist', description=' A utility that provides the history of all the jobs which has been submitted on all the clusters.'
                                                              ' Usage: python rsub.py joblist --to <cluster name>')
parser_joblist.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

args = parser.parse_args()

Schduler_ = scheduler("qsub", "qstat -xf", "qdel -w force", "user.palmetto.clemson.edu", "bramakr", "1")

Schduler_.from_json("config.json", args)