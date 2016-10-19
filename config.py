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

class Config(object):

    def from_json(self, config_json, args):
        with open(config_json, 'r') as f:
            params = json.load(f)
            for item in params["Resource"]:
                print(item['name'])
                if(args.to == item['name']):
                    subScheduler = item["scheduler"]
                    userName = item["userName"]
                    hostName =  item["hostName"]
                    remoteTmp = item["RemoteTmp"]
                    print(hostName)

                    resource_ = resource(userName,hostName, remoteTmp)
                    return resource_, subScheduler
