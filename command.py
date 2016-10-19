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
from history import history
from resource import resource
from submit import submit
from config import Config
from delete import delete
from query import query

class Command(object):

    def sel_cmd(self, subScheduler, resource_, args):
        Schduler_ = scheduler()

        if(subScheduler == "PBS"):
            subScheduler = PBS(Schduler_)
        elif(subScheduler == "Condor"):
            subScheduler = Condor(Schduler_)

        if args.cmd == 'submit':
            submit(subScheduler, resource_, args)
        elif args.cmd == 'delete':
            delete(subScheduler, resource_, args)
        elif args.cmd == 'query':
            query(subScheduler, resource_, args)
        elif args.cmd == 'history':
            history(args)

