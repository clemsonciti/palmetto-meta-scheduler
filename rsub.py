#!/usr/bin/env python
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
from history import history
from scheduler import scheduler
from config import Config
from job import job
from query import query
from query import parseQuery
from delete import delete
from delete import parseDelete
from history import parseHistory
from command import Command

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

parseQuery(subparsers)
parseDelete(subparsers)
parseHistory(subparsers)

args = parser.parse_args()

if args.cmd == 'submit':
    inputScriptFile = args.inFile
    clusterName     = args.to
    inpFiles        = args.transferInpFiles
    outFiles        = args.transferOutFiles
elif args.cmd == 'query' or args.cmd == 'delete':
    jobId = args.jobId

Config_ = Config()
resourceObj, subScheduler = Config_.from_json("config.json", args)

Command_ = Command()
Command_.sel_cmd(subScheduler, resourceObj, args)




