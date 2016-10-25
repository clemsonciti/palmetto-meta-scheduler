import fileinput
import sys
import os
import os.path
import json
import csv
import pickle
from time import sleep
import subprocess
import argparse
from subprocess import Popen, PIPE
from pprint import pprint

# Abstract class to hide the information of how the files are being transferred
class abstract(object):
    def __init__(self, transferType):
        self.transferType = transferType

    # Command to transfer files by the user
    def abstract_cmd(self, inputFile, host, path, Job_):
        Popen(['scp', inputFile, host + ':' + path], shell=False)
        if (Job_.transferInpFile is not None):
            for f in Job_.transferInpFile:
                print(f)
                Popen(['scp', f, host + ':' + path], shell=False)
        sleep(1)

    # Check the file transfer type and accordingly submit, delete or query
    # using that type. This provides flexibility for the user to provide commands
    # based on their needs
    def abstractType(self, cmd, host):
        if (self.transferType == 'ssh'):
            remoteId = subprocess.Popen(['ssh', host, cmd], shell=False, stdout=subprocess.PIPE)
            return remoteId
