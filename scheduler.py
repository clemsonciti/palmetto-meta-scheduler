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
from logger import Logger
from resource import resource

class scheduler(object):
    def __init__(self, submitCmd, statCmd, deleteCmd):
        self.submitCmd = submitCmd
        self.statCmd   = statCmd
        self.deleteCmd = deleteCmd

    # Submit function: This will take the inputs from the user and submits the
    # job on the palmetto. The list of files needed are copied from the local
    # machine to the palmetto node
    def Submit(self, args, Job_, resource_):
        path = resource_.remoteTmp
        host = resource_.userName + '@' + resource_.hostName
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

class PBS(scheduler):

    def __init__(self, scheduler):
        self.submitCmd = "qsub"
        self.statCmd   = "qstat -xf"
        self.deleteCmd = "qdel"

    def Submit(self, args, Job_, resource_):
        super(PBS, self).Submit(args, Job_, resource_)
        Logger_ = Logger()
        Job_.remoteId = Job_.remoteId.communicate()[0]
        if Job_.remoteId != '0':
            Logger_.map_job(args, Job_.remoteId)

    def Delete(self, args, Job_, resource_):
        super(PBS, self).Delete(args, Job_)
        host = resource_.userName + '@' + resource_.hostName
        print(host)
        qdelCmd = self.deleteCmd + ' ' + Job_.remoteId
        Popen(['ssh', host, qdelCmd], shell=False, stdout=PIPE)

    def Query(self, args, Job_, resource_):
        super(PBS, self).Query(args, Job_)
        host = resource_.userName + '@' + resource_.hostName
        qstatCmd = self.statCmd + ' ' + '-xf' + ' ' + Job_.remoteId
        Popen(['ssh', host, qstatCmd], shell=False)


class Condor(scheduler):
    def __init__(self, scheduler):
        self.submitCmd = "condor_submit"
        self.statCmd = "condor_q"
        self.deleteCmd = "condor_rm"

    def Submit(self, args, Job_, resource_):
        Logger_ = Logger()
        super(Condor, self).Submit(args, Job_, resource_)
        for line in Job_.remoteId.stdout:
            if "cluster" in line:
                Job_.remoteId = line.split("cluster", 1)[1]
                print(Job_.remoteId )
                if Job_.remoteId != '0':
                    Logger_.map_job(args, Job_.remoteId)

    def Delete(self, args, Job_, resource_):
        super(Condor, self).Delete(args, Job_)
        host = resource_.userName + '@' + resource_.hostName
        qdelCmd = self.deleteCmd + ' ' + Job_.remoteId
        print(self.deleteCmd)
        Popen(['ssh', host, qdelCmd], shell=False, stdout=PIPE)
        os.chdir('..')

    def Query(self, args, Job_, resource_):
        super(Condor, self).Query(args, Job_)
        host = resource_.userName + '@' + resource_.hostName
        qstatCmd = self.statCmd + ' ' + Job_.remoteId
        Popen(['ssh', host, qstatCmd], shell=False)
        os.chdir('..')