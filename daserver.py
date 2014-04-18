import sys
import string

class DAServer(object):
    """
    Base service class for DelegatingTCPServer. The protocol is for the server to server listen first.
    """

    def __init__(self, name_):
        self.name = name_
        self.log = lambda *args : sys.stdout.write(self.name + ': ' + string.join(map(str, args)) + '\n')

    def setLog(self, logFunc_):
        self.log = lambda *args : logFunc_(string.join(map(str, args)), name = self.name)

    def canServe(self, jobName_):
        return -1

    def serve(self, request_, jobName_):
        pass

