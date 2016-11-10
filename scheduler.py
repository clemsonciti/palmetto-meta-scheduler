import pickle
from subprocess import Popen
from logger import Logger
from abstract import abstract
import os

class scheduler(object):
    def __init__(self):
        self.submitCmd = "qsub"
        self.statCmd = "qstat -xf"
        self.deleteCmd = "qdel"

    # Submit function: This will take the inputs from the user and submits the
    # job on the palmetto. The list of files needed are copied from the local
    # machine to the palmetto node
    def Submit(self, fileName, Job_, resource_):
        path = resource_.remoteTmp
        host = resource_.userName + '@' + resource_.hostName
        splitJobScriptLocation = fileName.split('/')
        inputFile = splitJobScriptLocation[len(splitJobScriptLocation) - 1]
        qsubCmd = self.submitCmd + ' ' + path + '/' + inputFile
        abstract_ = abstract()
        abstract_.transferFiles(inputFile, host, path, Job_)
        Job_.remoteId = abstract_.fileTransferType(qsubCmd, host, resource_.transferType)

    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self, Job_, resource_):
        host = resource_.userName + '@' + resource_.hostName
        qdelCmd = self.deleteCmd + ' ' + Job_.remoteId
        abstract_ = abstract()
        abstract_.fileTransferType(qdelCmd, host, resource_.transferType)

    def Query(self, Job_, resource_):
        host = resource_.userName + '@' + resource_.hostName
        qstatCmd = self.statCmd + ' ' + Job_.remoteId
        abstract_ = abstract()
        abstract_.fileTransferType(qstatCmd, host, resource_.transferType)

class PBS(scheduler):

    def __init__(self, scheduler):
        self.submitCmd = "qsub"
        self.statCmd   = "qstat -xf"
        self.deleteCmd = "qdel"

    # Submit function: This will take the inputs from the user and submits the
    # job on the palmetto. The list of files needed are copied from the local
    # machine to the palmetto node
    def Submit(self, args, Job_, filename, resource_):
        fileName = args.inFile
        if os.path.splitext(fileName)[1] == ".pbs":
            super(PBS, self).Submit(fileName, Job_, resource_)
        elif os.path.splitext(fileName)[1] == ".submit":
            print("condor")
            fromCondortoPBS(fileName)
        Logger_ = Logger()
        for line in Job_.remoteId.stdout:
            Job_.remoteId =  line.rstrip()
        if Job_.remoteId != '0':
            with open(filename, 'wb') as f:
                pickle.dump(Job_, f)
            Logger_.map_job(args, filename)

    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self, args, Job_, resource_):
        super(PBS, self).Delete(Job_, resource_)

    def Query(self, args, Job_, resource_):
        super(PBS, self).Query(Job_, resource_)

class Condor(scheduler):
    def __init__(self, scheduler):
        self.submitCmd = "condor_submit"
        self.statCmd = "condor_q"
        self.deleteCmd = "condor_rm"

    # Submit function: This will take the inputs from the user and submits the
    # job on the OSG. The list of files needed are copied from the local
    # machine to the OSG node
    def Submit(self, args, Job_, filename, resource_):
        fileName = args.inFile
        Logger_ = Logger()
        if os.path.splitext(fileName)[1] == ".submit":
            super(Condor, self).Submit(fileName, Job_, resource_)
        elif os.path.splitext(fileName)[1] == ".pbs":
            file = Job_.fromPBStoCondor(fileName)
            super(Condor, self).Submit(file, Job_, resource_)

        for line in Job_.remoteId.stdout:
            if "cluster" in line:
                Job_.remoteId = line.split("cluster", 1)[1]
                Job_.remoteId = Job_.remoteId.rstrip()
                print(Job_.remoteId )
                if Job_.remoteId != '0':
                    with open(filename, 'wb') as f:
                        pickle.dump(Job_, f)
                    Logger_.map_job(args, filename)

    # Delete function: This will take the jobID as the input from the user and
    # deletes the particular job
    def Delete(self, Job_, resource_):
        super(Condor, self).Delete(Job_, resource_)

    def Query(self, Job_, resource_):
        super(Condor, self).Query(Job_, resource_)
