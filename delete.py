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

def delete(subparsers):
    parser_delete = subparsers.add_parser('delete', description=' A utility that allows you to delete a job that was submitted using submit command.'
                                                                ' Usage: python rsub.py delete --jobId <ID of the job>')

    parser_delete.add_argument('--jobId', metavar='<Local jobId>', required = True, help='The ID of the job which needs to be deleted')
    parser_delete.add_argument('--to', metavar='<clusterName>', required = True, help='The name of the cluster on which the job has to be submitted')
