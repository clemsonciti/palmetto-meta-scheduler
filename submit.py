#!/usr/bin/env python
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
from scheduler import scheduler
from scheduler import PBS
from scheduler import Condor
from job import job
from resource import resource
from config import Config

def submit(schduler, resource,  args):
    Job_ = job(0, 0, args.inFile, resource.remoteTmp, args.transferInpFiles, args.transferOutFiles)
    with open('data', 'w') as f:
        pickle.dump(Job_, f)

        schduler.Submit(args, Job_, resource)
