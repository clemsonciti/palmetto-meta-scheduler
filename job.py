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

# Creating a job object and initializing the job paramaters.
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

    def fromPBStoCondor(self, fileName):
        k = 0
        file = "PBStoCondor.submit"
        with open(fileName, 'r') as f:
            f1 = open(file, 'w')
            params = f.readlines()
            for line in params:
                print(line)
                if '#PBS' in line:
                    if k is 0:
                        f1.write(line.replace(line, "verse = vanilla" + '\n'))
                        f1.write("executable = hello.sh" + '\n')
                        f1.write("arguments = $(Process)" + '\n')
                        f1.write("error = err.$(Process)" + '\n')
                        f1.write("output = out.$(Process)" + '\n')
                        f1.write("log = log.$(Process)" + '\n')
                        f1.write("queue 10" + '\n')
                        k = k + 1;
                else:
                    f1.write(line)
        f1.close()
        f.close()
        return  file

    def fromCondortoPBS(self, fileName):
        with open(fileName, 'r') as f:
            params = f.readlines()
