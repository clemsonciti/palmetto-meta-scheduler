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
    def __init__(self, localId=None,  remoteId=None, jobName = None, RemoteTmp = None, transferInpFile = None, transferOutFile = None):
        self.localId = localId
        self.remoteId = remoteId
        self.jobName = jobName
        self.destPath = RemoteTmp
        self.transferInpFile = transferInpFile
        self.transferOutFile = transferOutFile

    def translateScript(self):
        print("translation")

    def jobFetch(self):
        print("Fetch")

class scheduler(object):
    def __init__(self, submitCmd, statCmd, deleteCmd, hostName, user):
        self.submitCmd = submitCmd
        self.statCmd   = statCmd
        self.deleteCmd = deleteCmd
        self.hostName  = hostName
        self.user      = user

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
        if(Job_.transferInpFile is not None):
            for f in Job_.transferInpFile:
                print(f)
                Popen(['scp', f, host + ':' + path], shell=False)
        sleep(1)
        Job_.remoteId = subprocess.Popen(['ssh', host, qsubCmd], shell=False, stdout=subprocess.PIPE)

    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self, args, Job_):
        input_file = 'map_jobid.csv'
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[0] == args.jobId and row[1] == args.to:
                    Job_.remoteId = row[2]
                    return

            print("Invalid jobId" + ' ' + args.jobId + ' ' + "to" + ' ' + args.to)
            sys.exit()

    def Query(self, args, Job_):
        input_file = 'map_jobid.csv'
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[0] == args.jobId:
                    Job_.remoteId = row[2]
                    break

    def from_json(self, config_json, args):
        with open(config_json, 'r') as f:
            params = json.load(f)
            if(args.to == "palmetto"):
                PBS_schduler_ = PBS(Schduler_)
                PBS_schduler_.user = params["Scheduler"][0]["userName"]
                PBS_schduler_.hostName = params["Scheduler"][0]["hostName"]
                RemoteTmp = params["Scheduler"][0]["RemoteTmp"]
                if args.cmd == 'submit':
                    print(args.transferInpFiles)
                    Job_ = job(0, 0, args.inFile, RemoteTmp, args.transferInpFiles, args.transferOutFiles)
                    PBS_schduler_.Submit(args, Job_)
                elif args.cmd == 'delete':
                    Job_ = job()
                    PBS_schduler_.Delete(args, Job_)
                elif args.cmd == 'query':
                    PBS_schduler_.Query(args)
                elif args.cmd == 'joblist':
                    History_ = History()
                    History_.Joblist(args)
            elif(args.to == "OSG"):
                Condor_schduler_ = Condor(Schduler_)
                Condor_schduler_.user = params["Scheduler"][1]["userName"]
                Condor_schduler_.hostName = params["Scheduler"][1]["hostName"]
                RemoteTmp = params["Scheduler"][0]["RemoteTmp"]
                if args.cmd == 'submit':
                    Job_ = job(0,0,args.inFile, RemoteTmp, args.transferInpFiles, args.transferOutFiles)
                    Condor_schduler_.Submit(args, Job_)
                elif args.cmd == 'delete':
                    Job_ = job()
                    Condor_schduler_.Delete(args, Job_)
                elif args.cmd == 'query':
                    Job_ = job()
                    Condor_schduler_.Query(args, Job_)
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
        Job_.remoteId = Job_.remoteId.communicate()[0]
        if Job_.remoteId != '0':
            Logger_.map_job(args, Job_.remoteId)

    def Delete(self, args, Job_):
        super(PBS, self).Delete(args, Job_)
        host = Schduler_.user + '@' + Schduler_.hostName
        print(host)
        qdelCmd = self.deleteCmd + ' ' + Job_.remoteId
        Popen(['ssh', host, qdelCmd], shell=False, stdout=PIPE)

    def Query(self, args, Job_):
        super(PBS, self).Query(args, Job_)
        host = self.user + '@' + self.hostName
        qstatCmd = self.statCmd + ' ' + '-xf' + ' ' + Job_.remoteId
        Popen(['ssh', host, qstatCmd], shell=False)


class Condor(scheduler):
    def __init__(self, scheduler):
        self.submitCmd = "condor_submit"
        self.statCmd = "condor_q"
        self.deleteCmd = "condor_rm"

    def Submit(self, args, Job_):
        Logger_ = Logger()
        super(Condor, self).Submit(args, Job_)
        for line in Job_.remoteId.stdout:
            if "cluster" in line:
                Job_.remoteId = line.split("cluster", 1)[1]
                print(Job_.remoteId )
                if Job_.remoteId != '0':
                    Logger_.map_job(args, Job_.remoteId)

    def Delete(self, args, Job_):
        super(Condor, self).Delete(args, Job_)
        host = self.user + '@' + self.hostName
        qdelCmd = self.deleteCmd + ' ' + Job_.remoteId
        print(self.deleteCmd)
        Popen(['ssh', host, qdelCmd], shell=False, stdout=PIPE)
        os.chdir('..')

    def Query(self, args, Job_):
        super(Condor, self).Query(args, Job_)
        host = self.user + '@' + self.hostName
        qstatCmd = self.statCmd + ' ' + Job_.remoteId
        Popen(['ssh', host, qstatCmd], shell=False)
        os.chdir('..')

# Class responsible for logging information about jobs
class Logger():

    def __init__(self):
        self.dd  = 0
        self.row = 0
        self.key = 0

    # This function maps the local ID with the respective job id and store the file locally
    def map_job(self, args, remoteId):

        output_file = "map_jobid.csv"

        if not os.path.isfile(output_file):
            writer = csv.writer(open(output_file, "w"))
            self.dd = 1
            writer.writerow([self.dd])
            writer.writerow(["tempJobId", "clusterName", "jobId", "scriptFiles"])
            writer.writerow([self.dd, args.to, remoteId, args.inFile])
        else:
            with open(output_file, 'r+') as csvfile:
                print("logger")
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
parser_submit.add_argument('--transferInpFiles', metavar='<Input files required by script file to run>', help='The extra files needed by the script file to run', nargs='+')
parser_submit.add_argument('--transferOutFiles', metavar='<Output files obtained after submitting the job>', help='The output files generated after submitting the job', nargs='+')


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

Schduler_ = scheduler("qsub", "qstat -xf", "qdel -w force", "user.palmetto.clemson.edu", "bramakr")

Schduler_.from_json("config.json", args)