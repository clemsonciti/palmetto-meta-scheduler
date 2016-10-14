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

class resource(object):
    def __init__(self, userName, hostName, remoteTmp):
        self.userName = userName
        self.hostName   = hostName
        self.remoteTmp = remoteTmp