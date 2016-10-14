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