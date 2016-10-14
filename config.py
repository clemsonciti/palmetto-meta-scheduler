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
from history import History
from resource import resource

Schduler_ = scheduler("qsub", "qstat -xf", "qdel -w force")

class Config(object):

    def from_json(self, config_json, args):
        with open(config_json, 'r') as f:
            params = json.load(f)
            if(args.to == "palmetto"):
                PBS_schduler_ = PBS(Schduler_)
                userName = params["Resource"][0]["userName"]
                hostName = params["Resource"][0]["hostName"]
                remoteTmp = params["Resource"][0]["RemoteTmp"]
                resource_ = resource(userName,hostName, remoteTmp)
                if args.cmd == 'submit':
                    Job_ = job(0, 0, args.inFile, remoteTmp, args.transferInpFiles, args.transferOutFiles)
                    PBS_schduler_.Submit(args, Job_, resource_)
                    with open('data', 'w') as f:
                        pickle.dump(Job_, f)
                    with open('data') as f:
                        y = pickle.load(f)
                        print(y)
                elif args.cmd == 'delete':
                    Job_ = job()
                    PBS_schduler_.Delete(args, Job_, resource_)
                elif args.cmd == 'query':
                    Job_ = job()
                    PBS_schduler_.Query(args, Job_, resource_)
                elif args.cmd == 'joblist':
                    History_ = History()
                    History_.Joblist(args)
            elif(args.to == "OSG"):
                Condor_schduler_ = Condor(Schduler_)
                userName = params["Resource"][1]["userName"]
                hostName = params["Resource"][1]["hostName"]
                remoteTmp = params["Resource"][0]["RemoteTmp"]
                resource_ = resource(userName, hostName, remoteTmp)
                if args.cmd == 'submit':
                    Job_ = job(0,0,args.inFile, remoteTmp, args.transferInpFiles, args.transferOutFiles)
                    Condor_schduler_.Submit(args, Job_, resource_)
                elif args.cmd == 'delete':
                    Job_ = job()
                    Condor_schduler_.Delete(args, Job_, resource_)
                elif args.cmd == 'query':
                    Job_ = job()
                    Condor_schduler_.Query(args, Job_, resource_)
                elif args.cmd == 'joblist':
                    History_ = History()
                    History_.Joblist(args)