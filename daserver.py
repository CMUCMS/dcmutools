import os
import sys
import string
import threading
import Queue

srcdir = os.path.dirname(os.path.realpath(__file__))
if srcdir not in sys.path:
    sys.path.append(srcdir)
from globals import *

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


class QueuedServer(DAServer):
    """
    Base class for servers using fixed- or indefinite-depth queue of requests.
    Derived class must define a static member MAXDEPTH.
    """

    def __init__(self, name_):
        DAServer.__init__(self, name_)
        
        self._queue = Queue.Queue(self.__class__.MAXDEPTH)
        self._lock = threading.Lock()
        
    def canServe(self, jobName_):
        if not self._queue.full(): return 1
        else: return 0

    def serve(self, request_, jobName_):
        self._queue.put(request_)
        if DEBUG: self.log('Queued job', jobName_)

        with self._lock:
            while True:
                try:
                    request = self._queue.get(block = False) # will raise Queue.Empty exception when empty
                    self._serveOne(request, jobName_)
                except Queue.Empty:
                    break
                except:
                    self.log('Communication with ' + jobName_ + ' failed: ' + str(sys.exc_info()[0:2]))
                    break
