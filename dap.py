#!/usr/bin/env python

### Distributed Asynchronous Processing
### Creates a job list from directories containing parallel-processable files and submits them
### to a batch system. Individual jobs are expected to download the files from one central
### node (where this script is running). The script then controls the download traffic and monitors
### the progress of the jobs.
### While the program was written for the above purpose and usage in CERN infrastructure
### in mind, the classes defined in this file can work in more general context.

import os
import sys
import copy
import glob
import random
import re
import time
import signal
import shutil
import string
import socket
import SocketServer
import subprocess
import threading
import Queue

PORT = 40000
DCMUHOST = "dcmu00"
TERMNODE = 'lxplus'
CMSSW_BASE = "/afs/cern.ch/user/y/yiiyama/cmssw/SLC6Ntuplizer5314"
HTMLDIR = '/var/www/html/jobstat'
USER = os.environ['USER']
try:
    TMPDIR = os.environ['TMPDIR']
except KeyError:
    TMPDIR = '/tmp/' + USER

DEBUG = False
SERVERONLY = False # for debugging purpose

def ignoreSIGINT():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

class DelegatingRequestHandler(SocketServer.BaseRequestHandler):
    """
    TCP request handler that delegates the actual tasks to registered servers.
    """

    def handle(self):
        try:
            jobName, service = self.server.requestManager.getReqInfo(self.request)
            if DEBUG: self.server.log('Passing', jobName, 'to', service.name)
            service.serve(self.request, jobName)
        except:
            if DEBUG: self.server.log('RequestHandler exception', sys.exc_info()[0:2])
            try:
                self.request.send('REJECT')
            except:
                pass
      

class DelegatingTCPServer(SocketServer.ThreadingTCPServer):
    """
    Threading TCP server with list of services to be provided. Each client must provide the verification key and declare the necessary
    service during the initial handshake.
    """

    class RequestManager(object):
        def __init__(self, server_):
            self._requests = {}
            self._lock = threading.Lock()
            self._server = server_
    
        def newRequest(self, request_, jobName_, service_):
            self._lock.acquire()
            self._requests[request_] = (jobName_, service_)
            self._lock.release()
    
        def checkoff(self, request_):
            jobName = 'unknown'
            service = 'unknown'
            self._lock.acquire()

            try:
                jobName, service = self._requests.pop(request_)
            except KeyError:
                pass
            except:
                self._server.log('Error checking off request:', sys.exc_info()[0:2])

            self._lock.release()

            self._server.log('Checked off request of', service.name, 'from', jobName)
    
        def getReqInfo(self, request_):
            self._lock.acquire()
            try:
                return self._requests[request_]
            except:
                return ('UNKNOWN', DAServer())
            finally:
                self._lock.release()


    def __init__(self, key_):

        if DEBUG: print 'Initializing ThreadingTCPServer'
        SocketServer.ThreadingTCPServer.__init__(self, ('', PORT), DelegatingRequestHandler)

        self._key = key_
        self._log = None
        self._services = {}

        self.requestManager = DelegatingTCPServer.RequestManager(self)

    def __del__(self):
        self.stop()

    def setLog(self, logFileName_):
        self._log = (open(logFileName_, 'a', 0), threading.Lock())

    def verify_request(self, request_, clientAddress_):
        request_.settimeout(180)
        try:
            self.log('Request received from', clientAddress_)

            key = request_.recv(1024)
            if key != self._key:
                raise RuntimeError("Wrong key")

            if DEBUG: self.log('Key verified for', clientAddress_)
        except:
            self.log('Handshake exception', sys.exc_info()[0:2])
            try:
                request_.send('REJECT')
            except:
                pass

            self.close_request(request_)
            
            return False

        return True

    def process_request_thread(self, request_, clientAddr_):
        try:
            request_.send('JOB')
            jobName = request_.recv(1024)
            request_.send('SVC')
            serviceName = request_.recv(1024)
            
            self.log('Job:', jobName, 'Service:', serviceName)
            
            service = self._services[serviceName]
            self.requestManager.newRequest(request_, jobName, service)
            
            vcode = service.canServe(jobName)
            if vcode == 1:
                request_.send('ACCEPT')

                if DEBUG: self.log('Handshake complete with', clientAddr_)

                # now delegate the actual task to services (RequestHandlerClass is instantiated here)
                self.finish_request(request_, clientAddr_)
            elif vcode == 0:
                request_.send('WAIT')
            else:
                raise RuntimeError(serviceName + ' cannot serve ' + jobName)

            # let the client close the connection first; frees up server from having to stay in TIME_WAIT
            request_.recv(1024)
        except:
            self.log('Process request exception', sys.exc_info()[0:2])
            try:
                request_.send('REJECT')
            except:
                pass

        self.close_request(request_)
        self.requestManager.checkoff(request_)

    def start(self):
        thread = threading.Thread(target = self.serve_forever, name = 'tcpServer')
        thread.start()

        if DEBUG: print 'started TCP server'

    def stop(self):
        self.server_close()
        while len(self.requestManager._requests):
            time.sleep(1)
        self.shutdown()

    def addService(self, service_):
        service_.setLog(self._log)
        self._services[service_.name] = service_

    def log(self, *args):
        if not self._log: return

        self._log[1].acquire()
        try:
            self._log[0].write(time.strftime('%H:%M:%S') + ': ')
            for arg in args:
                self._log[0].write(str(arg) + ' ')
            self._log[0].write('\n')
            self._log[0].flush()
            os.fsync(self._log[0])
        except:
            pass
        self._log[1].release()


class DAServer(object):
    """
    Base service class for DelegatingTCPServer. The protocol is for the server to server listen first.
    """

    def __init__(self, name_):
        self.name = name_
        
        self._log = None
        pass

    def setLog(self, log_):
        self._log = log_

    def canServe(self, jobName_):
        return -1

    def serve(self, request_, jobName_):
        pass

    def log(self, *args):
        if self._log:
            self._log[1].acquire()
            out = self._log[0]
        else:
            out = sys.stdout

        try:
            out.write(time.strftime('%H:%M:%S') + ' [' + self.name + ']: ')
            for arg in args:
                out.write(str(arg) + ' ')
            out.write('\n')
            out.flush()
            os.fsync(out)
        except:
            pass

        if self._log:
            self._log[1].release()


class DADispatcher(DAServer):
    """
    Service for job status bookkeeping.
    Protocol:
      SRV: send READY
      CLT: send START, EXIT, or DONE
    """

    states = [
        'UNKNOWN',
        'CREATED',
        'PENDING',
        'RUNNING',
        'DONE',
        'FAILED',
        'EXITED'
    ]

    class JobInfo(object):
        """
        Struct for job information.
        """
        
        def __init__(self):
            self.id = ''
            self.node = ''
            self.state = 'CREATED'
            self.proc = None

                
    def __init__(self, workspace_, processors_, terminal_ = None):
        DAServer.__init__(self, 'dispatch')
        
        self._workspace = workspace_
        self._processors = processors_
        self._jobNames = [] # List of job names. Read-only usage once all jobs are created.
        self._jobInfo = {}
        self._lock = threading.Lock()
        if terminal_:
            self._terminal = terminal_
        else:
            self._terminal = Terminal(TERMNODE)

    def __del__(self):
        if self._processors == 'bsub':
            for jobInfo in self._jobInfo.values():
                if jobInfo.state == 'RUNNING' or jobInfo.state == 'PENDING':
                    self._terminal.write('bkill {0}'.format(jobInfo.id))

        elif self._processors == 'lxplus':
            for jobInfo in self._jobInfo.values():
                if jobInfo.proc.isOpen():
                    jobInfo.proc.close(force = True)

        elif self._processors == 'local':
            for jobInfo in self._jobInfo.values():
                if jobInfo.proc.poll() is None:
                    jobInfo.proc.terminate()

    def canServe(self, jobName_):
        if jobName_ in self._jobNames: return 1
        else: return -1

    def serve(self, request_, jobName_):
        if jobName_ not in self._jobNames: return

        try:
            state = request_.recv(1024)
            self.log('Set state', jobName_, state)
        except:
            self._lock.acquire()
            self._jobInfo[jobName_].state = 'UNKNOWN'
            self._lock.release()
            return

        self._lock.acquire()
        self._jobInfo[jobName_].state = state
        self._lock.release()

        if state == 'FAILED':
            with open(self._workspace + '/logs/' + jobName_ + '.fail', 'w') as failLog:
                pass
        
    def createJob(self, jobName_):
        self._jobNames.append(jobName_)
        self._jobInfo[jobName_] = DADispatcher.JobInfo()

    def submitJob(self, jobName_, bsubOptions_ = '', setenv_ = ''):
        if jobName_ not in self._jobNames: return False

        self.log('Submitting job ', jobName_)

        if self._processors == 'bsub':
            command = "bsub {options} -J {jobName} -o {log} '{setenv}darun.py {workspace} {jobName}'".format(
                options = bsubOptions_,
                jobName = jobName_,
                log = self._workspace + '/logs/' + jobName_ + '.log',
                setenv = setenv_,
                workspace = self._workspace
            )

            self.log(command)
    
            bsubout = self._terminal.communicate(command)
                    
            if len(bsubout) == 0 or 'submitted' not in bsubout[0]:
                self.log('bsub failed')
                return False
                        
            matches = re.search('<([0-9]+)>', bsubout[0])
            if not matches:
                self.log('bsub returned', bsubout[0])
                return False
                        
            self.log('lxbatch job ID for {0} is {1}'.format(jobName_, matches.group(1)))
    
            self._lock.acquire()
            self._jobInfo[jobName_].id = matches.group(1)
            self._jobInfo[jobName_].state = 'PENDING'
            self._lock.release()

        elif self._processors == 'lxplus':
            command = '{setenv}darun.py {workspace} {jobName} > {log} 2>&1;exit'.format(
                setenv = setenv_,
                workspace = self._workspace,
                jobName = jobName_,
                log = self._workspace + '/logs/' + jobName_ + '.log'
            )

            self.log(command)

            term = Terminal(TERMNODE)
            term.write(command)

            self.log('Command issued to', term.node)

            self._lock.acquire()
            self._jobInfo[jobName_].id = jobName_
            self._jobInfo[jobName_].node = term.node
            self._jobInfo[jobName_].state = 'PENDING'
            self._jobInfo[jobName_].proc = term
            self._lock.release()

        elif self._processors == 'local':
            command = '{setenv}darun.py {workspace} {jobName}'.format(
                setenv = setenv_,
                workspace = self._workspace,
                jobName = jobName_
            )

            self.log(command)

            proc = subprocess.Popen(command,
                                    shell = True,
                                    stdin = PIPE,
                                    stdout = open(self._workspace + '/logs/' + jobName_ + '.log', 'w'),
                                    stderr = subprocess.STDOUT
                                )

            self.log('Subprocess started')

            self._lock.acquire()
            self._jobInfo[jobName_].id = jobName_
            self._jobInfo[jobName_].node = 'localhost'
            self._jobInfo[jobName_].state = 'PENDING'
            self._jobInfo[jobName_].proc = proc
            self._lock.release()

        else:
            return False

        return True

    def countJobs(self):
        jobCounts = dict((key, 0) for key in DADispatcher.states)

        self._lock.acquire()
        for jobInfo in self._jobInfo.values():
            jobCounts[jobInfo.state] += 1
        self._lock.release()

        return jobCounts

    def getJobs(self, states_):
        if type(states_) is not tuple:
            states_ = (states_,)

        self._lock.acquire()
        jobNames = [jobName for jobName, jobInfo in self._jobInfo.items() if jobInfo.state in states_]
        self._lock.release()

        return jobNames

    def queryJobStates(self):
        self._lock.acquire()

        try:
            if self._processors == 'bsub':
                lsfTracked = []
                lsfNodes = []
    
                response = self._terminal.communicate('bjobs')
    
                for line in response:
                    id = line.split()[0]
                    node = line.split()[5]
                    lsfTracked.append(id)
                    lsfNodes.append(node)

                # Two different tasks - if id is in the list of ids, set the node name
                # if not, the job may have exited abnormally - check state.
                for jobName, jobInfo in self._jobInfo.items():
                    if jobInfo.id in lsfTracked:
                        idx = lsfTracked.index(jobInfo.id)
                        jobInfo.node = lsfNodes[idx]
                    elif jobInfo.state == 'PENDING' or jobInfo.state == 'RUNNING':
                        self.log('Set state', jobName, 'EXITED')
                        jobInfo.state = 'EXITED'
                        with open(self._workspace + '/logs/' + jobName + '.fail', 'w') as failLog:
                            pass

            elif self._processors == 'lxplus':
                for jobName, jobInfo in self._jobInfo.items():
                    if jobInfo.state == 'RUNNING' and not jobInfo.proc.isOpen():
                        self.log('Set state', jobName, 'EXITED')
                        jobInfo.state = 'EXITED'
                        with open(self._workspace + '/logs/' + jobName + '.fail', 'w') as failLog:
                            pass
    
            elif self._processors == 'local':
                for jobName, jobInfo in self._jobInfo.items():
                    if jobInfo.state == 'RUNNING' and jobInfo.proc.poll() is not None:
                        self.log('Set state', jobName, 'EXITED')
                        jobInfo.state = 'EXITED'
                        with open(self._workspace + '/logs/' + jobName + '.fail', 'w') as failLog:
                            pass
        except:
            self.log('Job status query failed')
            
        self._lock.release()

    def printStatus(self, asstr = False):
        jobCounts = self.countJobs()

        line = ''
        for state in DADispatcher.states:
            line += ' {state}: {n}'.format(state = state, n = jobCounts[state])

        if asstr:
            return line
        else:
            line = '\r' + line
            line += ' ' * 10
            sys.stdout.write(line)
            if DEBUG: sys.stdout.write('\n')
            sys.stdout.flush()

    def printStatusWeb(self):
        for fileName in os.listdir(self._workspace + '/logs'):
            with open(self._workspace + '/logs/' + fileName, 'r') as source:
                with open(HTMLDIR + '/logs/' + fileName, 'w') as dest:
                    for line in source:
                        dest.write(line)

        statusStr = self.printStatus(asstr = True)

        self._lock.acquire()

        with open(HTMLDIR + '/index.html', 'w') as htmlFile:
            htmlFile.write('''
<html>
<head>
<meta http-equiv="refresh" content="21">
<title>''' + DCMUHOST + '''</title>
<style>
table,th,td
{
    border: 1px solid black;
}
table
{
    border-collapse: collapse;
}
</style>
</head>
<body>
<h1>''' + self._workspace + '''</h1>
<a href="logs/server.log">Server log</a><br>
''' + statusStr + '''
<table>
<tr><th>Name</th><th>ID</th><th>Node</th><th>State</th></tr>
''')

            for jobName in self._jobNames:
                jobInfo = self._jobInfo[jobName]
                htmlFile.write('<tr><td><a href="logs/{name}.log">{name}</a></td><td>{jobId}</td><td>{node}</td><td>{state}</td></tr>\n'.format(name = jobName, jobId = jobInfo.id, node = jobInfo.node, state = jobInfo.state))

            htmlFile.write('''
</table>
</body>
</html>
''')

        self._lock.release()
        
                
class DownloadRequestServer(DAServer):
    """
    Service for download traffic control.
    Protocol:
      SRV: send READY
      CLT: trasfer, send OK or FAIL (no action on server side in either case)
    """

    MAXDEPTH = 3

    def __init__(self, name_):
        DAServer.__init__(self, name_)
        
        self._queue = Queue.Queue(DownloadRequestServer.MAXDEPTH)
        self._lock = threading.Lock()
        
    def canServe(self, jobName_):
        if not self._queue.full(): return 1
        else: return 0

    def serve(self, request_, jobName_):
        self._queue.put(request_)
        if DEBUG: self.log('Queued job', jobName_)

        self._lock.acquire()
        while True:
            try:
                request = self._queue.get(block = False) # will raise Queue.Empty exception when empty
                while True:
                    response = request.recv(1024)
                    if response == 'DONE': break
                    request.send('GO')
            except:
                break

        self._lock.release()


class Reducer(DAServer):
    """
    Base class for reducer servers.
    Protocol:
      SRV: send target directory
      CLT: scp to target directory, send file name
    """

    def __init__(self, name_, targetFileName_, maxSize_, workdir_ = ''):
        DAServer.__init__(self, name_)
        
        self._targetFileBase = targetFileName_[0:targetFileName_.rfind('.')]
        self._targetFileSuffix = targetFileName_[targetFileName_.rfind('.'):]
        self._maxSize = maxSize_
        
        self._lock = threading.Lock()
        self._outputNumber = 0

        self.inputQueue = Queue.Queue()

        self.workdir = workdir_

        if self.workdir:
            try:
                os.mkdir(self.workdir + '/' + self.name)
            except OSError:
                pass
        else:
            while True:
                self.workdir = TMPDIR + '/' + string.join(random.sample(string.ascii_lowercase, 6), '')
                try:
                    os.mkdir(self.workdir)
                except OSError:
                    pass

        try:
            os.mkdir(self.workdir + '/input')
        except OSError:
            pass
        
        if DEBUG: self.log('Working directory', self.workdir)
        
        self.succeeded = []
        self.failed = []

    def canServe(self, jobName_):
        return 1

    def serve(self, request_, jobName_):
        try:
            request_.recv(1024)
            request_.send(self.workdir + '/input')
            response = request_.recv(1024)
            self.log('Job', jobName_, ':', response)
            if response == 'FAIL':
                raise RuntimeError('Copy failed')
        except:
            self.log('Job', jobName_, 'Reducer exception', sys.exc_info()[0:2])
            try:
                request_.send('FAILED')
            except:
                # should have a way to kill the job
                pass
            return

        request_.send('OK')
        if DEBUG: self.log('Adding', reponse, 'to reducer queue')
        self.inputQueue.put(response)

        self.reduce()

    def reduce(self):
        pass

    def copyOutputTo(self, destination_):
        if DEBUG: print 'Copying output to', destination_
        self.log('Copying output to', destination_)
        if self._outputNumber == 0:
            try:
                self.log('mv', self.workdir + '/' + self._targetFileBase + '_0' + self._targetFileSuffix, self.workdir + '/' + self._targetFileBase + self._targetFileSuffix)
                os.rename(self.workdir + '/' + self._targetFileBase + '_0' + self._targetFileSuffix,
                          self.workdir + '/' + self._targetFileBase + self._targetFileSuffix)
            except:
                self.log('Exception in renaming ouput:', sys.exc_info()[0:2])
                pass

        if ':' in destination_:
            for outputFile in glob.glob(self.workdir + '/' + self._targetFileBase + '*' + self._targetFileSuffix):
                proc = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet', outputFile, destination_],
                    stdin = subprocess.PIPE,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.STDOUT)
    
                while proc.poll() is None: time.sleep(10)

                if proc.returncode != 0:
                    while True:
                        line = proc.stdout.readline().strip()
                        if not line: break
                        self.log(line)

                    return False
            
            return True
            
        else:
            try:
                for outputFile in glob.glob(self.workdir + '/' + self._targetFileBase + '*' + self._targetFileSuffix):
                    os.rename(outputFile, destination_ + '/' + os.path.basename(outputFile))

                return True
            except:
                return False

    def cleanup(self):
        if len(self.failed) == 0:
            shutil.rmtree(self.workdir, ignore_errors = True)


class Hadder(Reducer):
    """
    Reducer using TFileMerger.
    """
    # TODO make this multi-thread

    ROOT = None

    def __init__(self, name_, targetFileName_, maxSize_, workdir_ = ''):
        if Hadder.ROOT is None:
            argv = sys.argv
            sys.argv = ['', '-b']
            import ROOT
            sys.argv = argv
            Hadder.ROOT = ROOT
        
        Reducer.__init__(self, name_, targetFileName_, maxSize_, workdir_)

        Hadder.ROOT.gSystem.Load("libTreePlayer.so")

    def reduce(self):
        self._lock.acquire()

        inputPaths = []
        while True:
            try:
                inputFile = self.inputQueue.get(block = False) # will raise Queue.Empty exception when empty
                inputPaths.append(self.workdir + '/input/' + inputFile)
            except Queue.Empty:
                break

        tmpOutputPath = self.workdir + '/tmp' + self._targetFileSuffix

        while len(inputPaths):
            merger = Hadder.ROOT.TFileMerger(False, False)
    
            if not merger.OutputFile(tmpOutputPath):
                self.log('Cannot open temporary output', tmpOutputPath)
                self.failed += inputPaths
                return
    
            outputPath = self.workdir + '/' + self._targetFileBase + '_' + str(self._outputNumber) + self._targetFileSuffix
    
            if os.path.exists(outputPath):
                inputPaths.insert(0, outputPath)
    
            self.log('Merging', inputPaths)
    
            totalSize = 0
            toAdd = []
            for inputPath in inputPaths:
                if DEBUG: print 'Reduce:', inputPath
    
                if not merger.AddFile(inputPath):
                    self.log('Cannot add', inputPath, 'to list')
                    self.failed.append(inputPath)
                    continue
    
                totalSize += os.stat(inputPath).st_size
                toAdd.append(inputPath)
    
                if totalSize / 1048576 >= self._maxSize: break
            else:
                # loop over inputPaths reached the end -> no need to increment outputNumber
                self._outputNumber -= 1

            inputPaths = inputPaths[inputPaths.index(inputPath) + 1:]

            self.log('hadd', tmpOutputPath, string.join(toAdd))
            result = merger.Merge()

            if '/input/' not in toAdd[0]: # first element was the previous output
                toAdd.pop(0)
            
            if result:
                self.log('mv', tmpOutputPath, outputPath)
                os.rename(tmpOutputPath, outputPath)
                self.succeeded += toAdd
            else:
                self.log('Merge failed')
                self.failed += toAdd

            self._outputNumber += 1

            totalSize = 0
            added = []
    
        self._lock.release()

            
class Terminal:
    """
    A wrapper for an ssh session.
    """

    def __init__(self, servName_):
        self._servName = servName_
        self._session = None
        self.node = ''
        self.open()

    def __del__(self):
        self.close()

    def open(self):
        if self.isOpen(): return
        
        self._session = subprocess.Popen(['ssh', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet', '-T', self._servName],
            stdin = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT,
            preexec_fn = ignoreSIGINT)
        
        self.node = self.communicate('echo $HOSTNAME')[0]
        print 'Terminal opened on ' + self.node
        
    def close(self, force_ = False):
        if not self.isOpen(): return
        
        try:
            if force_: self._session.terminate()
            else: self._session.stdin.write('exit\n')
            while self._session.poll() is None: pass
            self.node = ''
        except:
            print 'Failed to close SSH connection'

    def isOpen(self):
        return self._session and self._session.poll() is None

    def write(self, line_):
        try:
            self._session.stdin.write(line_.strip() + '\n')
        except:
            print 'Failed to write {0} to terminal'.format(line_.strip()), sys.exc_info()[0:2]
            self.close(True)
            self.open()

    def read(self):
        response = ''
        try:
            response = self._session.stdout.readline().strip()
        except:
            print 'Failed to read from terminal', sys.exc_info()[0:2]
            self.close(True)
            self.open()

        return response

    def communicate(self, inputs_):
        output = []
        if DEBUG: print 'communicate: ', inputs_
        try:
            if isinstance(inputs_, list):
                for line in inputs_:
                    self._session.stdin.write(line.strip() + '\n')
            elif isinstance(inputs_, str):
                self._session.stdin.write(inputs_.strip() + '\n')

            self._session.stdin.write('echo EOL\n')

            while True:
                line = self._session.stdout.readline().strip()
                if line == 'EOL': break
                output.append(line)
        except:
            print 'Communication with terminal failed: ', sys.exc_info()[0:2]
            self.close(True)
            self.open()

        return output


if __name__ == '__main__':

    try:
        if DCMUHOST not in os.environ['HOSTNAME']:
            raise EnvironmentError
    except:
        print "This is not ", DCMUHOST
        raise

    for pid in [pid for pid in os.listdir('/proc') if pid.isdigit()]:
        try:
            if 'dap.py' in open(os.path.join('/proc', pid, 'cmdline', 'rb')).read():
                print 'dap already running with pid', pid
                raise EnvironmentalError
        except IOError:
            pass

    SETENV = 'cd ' + CMSSW_BASE + ';eval `scramv1 runtime -sh`'

    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage="usage: %prog [options] dataset [dataset2 [dataset3 ...]] macro")

    execOpts = OptionGroup(parser, "Job execution options", "These options will be saved in the job configuration and will be used at each job execution.")
    execOpts.add_option("-w", "--workspace", dest='workspace', help='Name of the job directory', default="", metavar="DIR")
    execOpts.add_option("-e", "--setenv", dest='setenv', help='Command to issue before running the job. Default sets CMSSW environment for bash.', default=SETENV, metavar="CMD")
    execOpts.add_option("-a", "--analyzer-arguments", dest='analyzerArguments', help="Arguments to be passed to the initialize() function of the analyzer object.", default="", metavar="ARGS")
    execOpts.add_option("-I", "--include", dest="includePaths", help="Include path for compilation", default="", metavar="-IDIR1 [-IDIR2 [-IDIR3 ...]]")
    execOpts.add_option("-l", "--lib", dest="libraries", help="Libraries to load", default="", metavar="LIB1[,LIB2[,LIB3...]]")
    execOpts.add_option("-d", "--output-dir", dest="outputDir", help="Output [host:]directory", default="./", metavar="DIR")
    execOpts.add_option("-u", "--reducer", dest="reducer", help="Reducer module. Set to None to disable reducer. Default is Hadder.", default="Hadder", metavar="MODULE")
    execOpts.add_option("-o", "--output-file", dest="outputFile", help="Output file name. Ignored when reducer is None", default="", metavar="OUT")
    execOpts.add_option("-m", "--max-size", dest="maxSize", help="Approximate maximum size in MB of the reducer output.", default=1024, metavar="SIZE")
    parser.add_option_group(execOpts)

    inputOpts = OptionGroup(parser, "Input options", "These options are used at job creation time to configure the input.")
    inputOpts.add_option("-n", "--files-per-job", type="int", dest='filesPerJob', help='Number of files per job', metavar="NUM", default=8)
    #    inputOpts.add_option("-x", "--external-list", dest='externalList', help="External list of strings to be used for job scheduling")
    inputOpts.add_option("-f", "--file-format", dest="nameFormat",
        help="""\
Wildcard expression of the name of the files in the dataset to use. Multiple files (separated by comma) can be related through the wildcard character.
Each instance of the match is passed to the worker function. Example: susyEvents*.root,susyTriggers*.root""",
        default="*.root", metavar="FORMAT")
    #    inputOpts.add_option("-z", "--no-suffix", action="store_true", dest="noSuffix", help="Do not append the jobId to the output file names")
    parser.add_option_group(inputOpts)

    runtimeOpts = OptionGroup(parser, "Runtime options", "These options can be changed for each job submission.")
    runtimeOpts.add_option('-c', '--client', dest='client', help="Client to use for processing. Options are bsub (lxbatch), lxplus, and local.", default='bsub', metavar="MODE")
    runtimeOpts.add_option("-b", "--bsub-options", dest="bsubOptions", help='Options to pass to bsub command. -J and -cwd are set automatically. Example: -R "rusage[pool=2048]" -q 1nh', metavar="OPTIONS", default="-q 1nh")
    runtimeOpts.add_option("-D", "--debug", action="store_true", dest="debug", help="")
    runtimeOpts.add_option("-r", "--resubmit", action="store_true", dest="resubmit", help="Resubmit the job")
    runtimeOpts.add_option("-R", "--recover", action="store_true", dest="recover", help="Recover failed jobs")
    runtimeOpts.add_option("-M", "--max-jobs", dest="maxJobs", help="Maximum (approximate) number of concurrent jobs to be submitted", type="int", default=30, metavar="NUM")
    runtimeOpts.add_option("-j", "--jobs", dest="jobs", help="Jobs to submit", default="", metavar="JOB1[,JOB2[,JOB3...]]")
    runtimeOpts.add_option("-S", "--auto-resubmit", action="store_true", dest="autoResubmit", help="Automatically resubmit failed jobs")
    runtimeOpts.add_option("-t", "--no-submit", action="store_true", dest="noSubmit", help="Compile and quit")
    parser.add_option_group(runtimeOpts)

    (options, args) = parser.parse_args()

    if options.client not in ['bsub', 'lxplus', 'local']:
        raise RuntimeError('Client ' + options.client + ' not supported')

    sys.argv = sys.argv[0:1]

    DEBUG = options.debug
    
    resubmit = options.resubmit or options.recover

    if not resubmit and (len(args) < 2 or not options.workspace):
        parser.print_usage()
        sys.exit(1)

    ### OPEN TERMINAL ###

    terminal = Terminal(TERMNODE)

    setenv = options.setenv.strip().rstrip(';')
    if setenv:
        setenv += '; '

    ### CREATE SERVER ###

    jobKey = string.join(random.sample(string.ascii_lowercase, 4), '')

    tcpServer = DelegatingTCPServer(jobKey)

    ### CONFIGURATION ###

    if resubmit:
        workspace = os.path.realpath(args[0])
        if not os.path.exists(workspace) or not os.path.exists(workspace + '/jobconfig.py'):
            raise RuntimeError("{0} is not a valid workspace".format(workspace))
        
        sys.path.append(workspace)
        import jobconfig
        jobConfig = copy.copy(jobconfig.jobConfig)

        if not options.recover:
            ilog = 1
            while True:
                if os.path.isdir(workspace + '/logs_' + str(ilog)):
                    ilog += 1
                else:
                    break
            os.rename(workspace + '/logs', workspace + '/logs_' + str(ilog))
            os.mkdir(workspace + '/logs')
    else:
        workspace = os.path.realpath(options.workspace)
        if os.path.exists(workspace):
            raise RuntimeError("{0} already exists".format(workspace))

        jobConfig = {}

        jobConfig["workspace"] = workspace

        macro = args[len(args) - 1]
        if ':' in macro:
            jobConfig["macro"] = os.path.realpath(macro.split(':')[0])
            jobConfig["analyzer"] = macro.split(':')[1]
        else:
            jobConfig["macro"] = macro
            jobConfig["analyzer"] = macro[macro.rfind('/') + 1:macro.rfind('.')]

        jobConfig["includePaths"] = options.includePaths.strip()

        if options.libraries.strip():
            jobConfig["libraries"] = libraries
        else:
            jobConfig["libraries"] = []

        analyzerArguments = []
        if options.analyzerArguments.strip():
            analyzerArguments = options.analyzerArguments.strip().split(',')

        outputDir = options.outputDir.strip()
        if ':' in outputDir:
            jobConfig["outputNode"] = outputDir.split(':')[0]
            outputDir = outputDir.split(':')[1]
        else:
            jobConfig["outputNode"] = os.environ['HOSTNAME']
        
        jobConfig["outputDir"] = outputDir
        jobConfig["outputFile"] = options.outputFile.strip()
        jobConfig["reducer"] = options.reducer.strip()
        jobConfig["maxSize"] = options.maxSize
        jobConfig["setenv"] = setenv

    print 'Using {0} as workspace'.format(workspace)

    ### RUNTIME-SPECIFIC CONFIGURATIONS ###

    jobConfig['key'] = jobKey
    jobConfig['serverHost'] = os.environ['HOSTNAME']
    jobConfig['serverPort'] = tcpServer.server_address[1]
    jobConfig['serverTmpDir'] = TMPDIR

    ### OPEN WORKSPACE ###

    if not resubmit:
        os.mkdir(workspace)
        os.mkdir(workspace + '/inputs')
        os.mkdir(workspace + '/logs')

    with open(workspace + '/jobconfig.py', 'w') as configFile:
        configFile.write('jobConfig = ' + str(jobConfig))

    tmpWorkspace = TMPDIR + '/' + jobKey
    os.mkdir(tmpWorkspace)

    ### SERVER LOG ###

    tcpServer.setLog(workspace + '/logs/server.log')

    ### JOB LIST ###
    
    if DEBUG: print "preparing the lists of input files"

    if options.resubmit:
        allJobs = map(os.path.basename, glob.glob(workspace + '/inputs/*'))
    elif options.recover:
        allJobs = map(lambda name: os.path.basename(name).replace(".fail", ""), glob.glob(workspace + "/logs/*.fail"))        
    else:        
        if not options.nameFormat:
            raise RuntimeError("Input file name format")
        
        nameSpecs = map(str.strip, options.nameFormat.split(','))
        mainName = nameSpecs[0]
        mainLfns = []

        datasets = args[0:len(args) - 1]

        for dataset in datasets:
            mainLfns += glob.glob(dataset + '/' + mainName)
        
        pfns = []
        for lfn in mainLfns:
            try:
                pfn = os.readlink(lfn)
                subst = re.search(mainName.replace('*', '(.*)'), lfn).group(1)
                for nameSpec in nameSpecs[1:]:
                    pfn += ',' + os.readlink(os.path.dirname(lfn) + '/' + nameSpec.replace('*', subst))
            except OSError:
                continue

            pfns.append(pfn)

        allJobs = []
        iJob = 0
        while len(pfns):
            jobName = str(iJob)
            allJobs.append(jobName)
            try:
                with open(workspace + '/inputs/' + jobName, 'w') as inputList:
                    for f in range(options.filesPerJob):
                        inputList.write(pfns.pop(random.randint(0, len(pfns) - 1)) + '\n')
            except ValueError:
                break
                    
            iJob += 1

    if options.jobs:
        jobFilter = options.jobs.split(',')
    else:
        jobFilter = []
            
    if len(jobFilter) > 0:
        filtered = []
        for filt in jobFilter:
            filtered += filter(lambda name: re.match('^' + filt.replace('*', '.*') + '$', name) is not None, allJobs)

        allJobs = filtered

    if len(allJobs) == 0:
        print 'no matching jobs found. exiting.'
        sys.exit(0)

    try:
        allJobs.sort(lambda x, y : int(x) - int(y))
    except ValueError:
        allJobs.sort()

    ### MACRO ###

    print 'Checking ' + jobConfig['macro']
    with open(jobConfig['macro']) as macroFile:
        for line in macroFile:
            if re.match('^[ ]*class[ ]+' + jobConfig['analyzer'], line): break
        else:
            raise RuntimeError("analyzer not found in macro")

    if not resubmit:
        # compile the source in the terminal
        if DEBUG: print "writing compile code"

        with open(workspace + '/macro.py', 'w') as configFile:
            configFile.write('import sys\n')
            configFile.write('sys.argv = ["", "-b"]\n')
            configFile.write('import ROOT\n')
            configFile.write('rootlogon = ROOT.gEnv.GetValue("Rint.Logon", "")\n')
            configFile.write('if rootlogon:\n')
            configFile.write('    ROOT.gROOT.Macro(rootlogon)\n')
            for lib in jobConfig['libraries']:
                configFile.write('ROOT.gSystem.Load("' + lib + '")\n')
            configFile.write('ROOT.gSystem.AddIncludePath("' + jobConfig["includePaths"] + '")\n')
            configFile.write('if ROOT.gROOT.LoadMacro("' + jobConfig['macro'] + '+") != 0: sys.exit(1)\n')

            configFile.write('arguments = (')
            for arg in analyzerArguments:
                arg = arg.strip()
                if not arg: continue
                try:
                    eval(arg)
                    configFile.write(arg + ', ')
                except NameError:
                    if arg == 'true':
                        configFile.write('True, ')
                    elif arg == 'false':
                        configFile.write('False, ')
                    else:
                        # likely is an enum defined in the macro
                        configFile.write('getattr(ROOT, "' + arg + '"), ')

            configFile.write(')\n')
    
    terminal.communicate(jobConfig['setenv'] + 'cd ' + workspace + ';python macro.py > logs/compile.log 2>&1')

    with open(workspace + '/logs/compile.log', 'r') as logFile:
        for line in logFile:
            if 'Error' in line or 'fail' in line:
                raise RuntimeError("Compilation failed")

    ### SERVICES ###
        
    # disk names
    diskNames = map(lambda p: p.replace('/data/', ''), glob.glob('/data/*'))

    # download request handlers
    for diskName in diskNames:
        tcpServer.addService(DownloadRequestServer(diskName))

    # reducers
    reducer = None
    if jobConfig['reducer'] != 'None':
        if not jobConfig['outputFile']:
            raise RuntimeError('Reducer requires output file name specification')

        reducer = eval(jobConfig['reducer'])('reduce', jobConfig['outputFile'], jobConfig['maxSize'], tmpWorkspace)
        reducer.setLog(tcpServer._log)
#        tcpServer.addService(reducer)
# Choosing to run reducer "offline" and not as a service; adds stability with a price of little time rag after the jobs are done. darun jobs will upload the output to $TMPDIR/{key}/reduce

    # dispatcher
    dispatcher = DADispatcher(workspace, options.client, terminal)

    for jobName in allJobs:
        dispatcher.createJob(jobName)

    tcpServer.addService(dispatcher)

    # start the TCP server
    tcpServer.start()

    ### JOB SUBMISSION LOOP ###

    if SERVERONLY:
        print 'Type quit to stop:'
        response = ''
        try:
            while response != 'quit':
                response = sys.stdin.readline().strip()
        except:
            pass
        
        sys.exit(0)

    if options.noSubmit:
        print 'no-submit flag is set. exiting.'
        sys.exit(0)

    print 'Start job submission'

    while True:
        dispatcher.printStatus()
        dispatcher.printStatusWeb()
        
        jobCounts = dispatcher.countJobs()

        if options.autoResubmit:
            if jobCounts['DONE'] + jobCounts['UNKNOWN'] == len(allJobs): break
        else:
            if jobCounts['DONE'] + jobCounts['FAILED'] + jobCounts['EXITED'] + jobCounts['UNKNOWN'] == len(allJobs): break
        
        if options.autoResubmit:
            mask = ('CREATED', 'FAILED', 'EXITED')
        else:
            mask = 'CREATED'

        subLimit = options.maxJobs - jobCounts['PENDING'] - jobCounts['RUNNING']

        if subLimit > 0:
            for jobName in dispatcher.getJobs(mask)[0:subLimit]:
                dispatcher.submitJob(jobName, options.bsubOptions, jobConfig['setenv'])

        time.sleep(20)

        dispatcher.queryJobStates()

    # stop the TCP server
    tcpServer.stop()

    dispatcher.printStatus()
    dispatcher.printStatusWeb()

    print ''

    if jobCounts['DONE'] != len(allJobs):
        print 'There are failed jobs.'
        sys.exit(1)

    ### REDUCER ###

    if jobConfig['reducer'] != 'None':
        print 'Reducing output'
        
        outputFiles = glob.glob(reducer.workdir + '/input/*')
        for outputFile in outputFiles:
            reducer.inputQueue.put(os.path.basename(outputFile))

        reducer.reduce()

        if reducer.copyOutputTo(jobConfig['outputNode'] + ':' + jobConfig['outputDir']):
            if len(reducer.succeeded) != len(allJobs):
                print 'Number of input files to reducer does not match the number of jobs: {0}/{1}'.format(len(reducer.succeeded), len(allJobs))
            elif len(reducer.failed):
                print 'Final reduction failed. Output in', reducer.workdir
            else:
                reducer.cleanup()
        else:
            print 'Copy to ' + jobConfig['outputNode'] + ':' + jobConfig['outputDir'] + ' failed.'

    ### CLEANUP ###
            
    shutil.rmtree(tmpWorkspace, ignore_errors = True)

    print 'Done.'
