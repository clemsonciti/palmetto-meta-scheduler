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
from submit import submit

def parseDelete(subparsers):
    parser_delete = subparsers.add_parser('delete', description=' A utility that allows you to delete a job that was submitted using submit command.'
                                                                ' Usage: python rsub.py delete --jobId <ID of the job>')

    parser_delete.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job which needs to be deleted')
    parser_delete.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')

def delete(schduler, resource, args):
    with open('data') as f1:
        Job_ = pickle.load(f1)
        print(Job_)
        schduler.Delete(args, Job_, resource)