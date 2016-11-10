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

# Config function to read config data provided by user
class Config(object):

    # This function loads the data provided by the user and assigns it to the username, hostname etc.
    # Resource object is also created with initializing these parameters
    def from_json(self, config_json, clusterName):
        with open(config_json, 'r') as f:
            params = json.load(f)
            if clusterName is None:
                for item in params["default"]:
                    clusterName = item["name"]

            for item in params["Resource"]:
                if(clusterName == item['name']):
                    subScheduler = item["scheduler"]
                    userName = item["userName"]
                    hostName =  item["hostName"]
                    remoteTmp = item["RemoteTmp"]
                    transferType = item["transferType"]

                    resource_ = resource(userName, hostName, remoteTmp, transferType)
                    return resource_, subScheduler
