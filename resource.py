import sys
import os

# Resource class which constains the information provided by the user.
class resource(object):
    def __init__(self, userName, hostName, remoteTmp):
        self.userName = userName
        self.hostName   = hostName
        self.remoteTmp = remoteTmp