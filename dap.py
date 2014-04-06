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
HTMLDIR = '/afs/cern.ch/user/y/yiiyama/www/dcmu00'
USER = os.environ['USER']
try:
    TMPDIR = os.environ['TMPDIR']
except KeyError:
    TMPDIR = '/tmp/' + USER

DEBUG = False
SERVERONLY = False # for debugging purpose


class DelegatingTCPServer(SocketServer.ThreadingTCPServer):
    """
    Threading TCP server with list of services to be provided. Each client must provide the verification key and declare the necessary
    service during the initial handshake.
    The implementation does not follow the original design concept of SocketServer in that no RequestHandlerClass is used.
    This is to eliminate the communication cost between the server and the handler object.
    """

    def __init__(self, key_):
        if DEBUG: print 'Initializing ThreadingTCPServer'
        SocketServer.ThreadingTCPServer.__init__(self, ('', PORT), None)

        self._key = key_
        self._logLines = Queue.Queue()
        self._log = sys.stdout
        self._services = {}
        self._running = False

        threading.Thread(target = self._writeLog, name = 'serverLog').start()

    def __del__(self):
        self.stop()

    def setLog(self, logFileName_):
        self._log = open(logFileName_, 'a', 0)

    def verify_request(self, request_, clientAddress_):
        request_.settimeout(180)
        try:
            self.log('Request received from' + str(clientAddress_))

            key = request_.recv(1024)
            if key != self._key:
                raise RuntimeError("Wrong key")

            if DEBUG: self.log('Key verified for ' + str(clientAddress_))
        except:
            self.log('Handshake exception ' + str(sys.exc_info()[0:2]))
            try:
                request_.send('REJECT')
            except:
                pass

            self.close_request(request_)
            
            return False

        return True

    def process_request_thread(self, request_, clientAddr_):
        jobName = 'unknown'
        serviceName = 'unknown'
        try:
            request_.send('JOB')
            jobName = request_.recv(1024)
            request_.send('SVC')
            serviceName = request_.recv(1024)
            
            self.log('Job: ' + jobName + ' Service: ' + serviceName)
            
            service = self._services[serviceName]
            
            vcode = service.canServe(jobName)
            if vcode == 1:
                request_.send('ACCEPT')

                if DEBUG: self.log('Handshake complete with ' + str(clientAddr_))

                # now delegate the actual task to service
                try:
                    if DEBUG: self.server.log('Passing ' + jobName + ' to ' + serviceName)
                    service.serve(request_, jobName)
                except:
                    if DEBUG: self.server.log('Service exception ' + str(sys.exc_info()[0:2]))
                    try:
                        request_.send('REJECT')
                    except:
                        pass

            elif vcode == 0:
                request_.send('WAIT')
            else:
                raise RuntimeError(serviceName + ' cannot serve ' + jobName)

            # let the client close the connection first; frees up server from having to stay in TIME_WAIT
            request_.recv(1024)
        except:
            self.log('Process request exception ' + str(sys.exc_info()[0:2]))
            try:
                request_.send('REJECT')
            except:
                pass

        self.close_request(request_)
        self.log('Closed request from ' + jobName + ' for ' + serviceName)

    def start(self):
        thread = threading.Thread(target = self.serve_forever, name = 'tcpServer')
        thread.daemon = True
        thread.start()
        self._running = True

        if DEBUG: print 'started TCP server'

    def stop(self):
        self.log('Stopping server..')
        if self._running: self.shutdown()
        self.server_close()
        self.log('Server stopped.')
        self._running = False
        self._logLines.put('EOF')

    def addService(self, service_):
        service_.setLog(self.log)
        self._services[service_.name] = service_

    def log(self, linestr_, name = ''):
        line = time.strftime('%H:%M:%S')
        if name:
            line += ' [' + name + ']'
        line += ': ' + linestr_
        self._logLines.put(line)

    def _writeLog(self):
        while True:
            line = self._logLines.get()
            if line == 'EOF': break
            self._log.write(line + '\n')
            self._log.flush()
            os.fsync(self._log)


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
        
        def __init__(self, name_):
            self.name = name_
            self.id = ''
            self.node = ''
            self.state = 'CREATED'
            self.proc = None

                
    def __init__(self, workspace_, dispatchMode_, resubmit = False, terminal = None):
        DAServer.__init__(self, 'dispatch')
        
        self._workspace = workspace_
        self._dispatchMode = dispatchMode_
        self._jobInfo = {}
        self._resubmit = resubmit
        self._readyJobs = []
        self._runningJobs = []
        self._lock = threading.Lock()
        self._update = threading.Event()
        if terminal:
            self._terminal = terminal
        else:
            self._terminal = Terminal(TERMNODE)

    def __del__(self):
        if self._dispatchMode == 'bsub':
            for jobInfo in self._jobInfo.values():
                if jobInfo.state == 'RUNNING' or jobInfo.state == 'PENDING':
                    self._terminal.write('bkill {0}'.format(jobInfo.id))

        elif self._dispatchMode == 'lxplus':
            for jobInfo in self._jobInfo.values():
                if jobInfo.proc.isOpen():
                    jobInfo.proc.close(force = True)

        elif self._dispatchMode == 'local':
            for jobInfo in self._jobInfo.values():
                if jobInfo.proc.poll() is None:
                    jobInfo.proc.terminate()

    def canServe(self, jobName_):
        if jobName_ in self._jobInfo: return 1
        else: return -1

    def serve(self, request_, jobName_):
        if jobName_ not in self._jobInfo: return

        try:
            state = request_.recv(1024)
            self.log('Set state', jobName_, state)
        except:
            state = 'UNKNOWN'

        with self._lock:
            try:
                jobInfo = self._jobInfo[jobName_]
        
                jobInfo.state = state
    
                if state == 'DONE':
                    self._runningJobs.remove(jobInfo)
                elif state == 'FAILED':
                    self._runningJobs.remove(jobInfo)
                    if self._resubmit:
                        self._readyJobs.append(jobInfo)
            except:
                self.log('Exception while serving', jobName_, sys.exc_info()[0:2])

        if state == 'FAILED':
            with open(self._workspace + '/logs/' + jobName_ + '.fail', 'w') as failLog:
                pass

        self._update.set()
        
    def createJob(self, jobName_):
        self._jobInfo[jobName_] = DADispatcher.JobInfo(jobName_)
        self._readyJobs.append(self._jobInfo[jobName_])

    def submitOne(self, bsubOptions = '', setenv = '', logdir = ''):
        if len(self._readyJobs):
            jobInfo = self._readyJobs[0]
            self.submit(jobInfo, bsubOptions, setenv, logdir)

    def submit(self, jobInfo_, bsubOptions = '', setenv = '', logdir = ''):
        with self._lock:
            try:
                self._readyJobs.remove(jobInfo_)
            except ValueError:
                return
                
            self._runningJobs.append(jobInfo_)
        
        self.log('Submitting job ', jobInfo_.name)

        if not logdir:
            logdir = self._workspace + '/logs'

        if self._dispatchMode == 'bsub':
            command = "bsub {options} -J {jobName} -o {log} '{setenv}darun.py {workspace} {jobName}'".format(
                options = bsubOptions,
                jobName = jobInfo_.name,
                log = logdir + '/' + jobInfo_.name + '.log',
                setenv = setenv,
                workspace = self._workspace
            )

            self.log(command)
    
            bsubout = self._terminal.communicate(command)

            success = False
            if len(bsubout) != 0 and 'submitted' in bsubout[0]:
                matches = re.search('<([0-9]+)>', bsubout[0])
                if matches:
                    success = True

            if not success:
                self.log('bsub failed')
                with self._lock:
                    self._runningJobs.remove(jobInfo_)
                    self._readyJobs.append(jobInfo_)

                return

            self.log('lxbatch job ID for {0} is {1}'.format(jobInfo_.name, matches.group(1)))

            jobId = matches.group(1)
            node = ''
            proc = None

        elif self._dispatchMode == 'lxplus':
            command = '{setenv}darun.py {workspace} {jobName} > {log} 2>&1;exit'.format(
                setenv = setenv,
                workspace = self._workspace,
                jobName = jobInfo_.name,
                log = logdir + '/' + jobInfo_.name + '.log'
            )

            self.log(command)

            term = Terminal(TERMNODE)
            term.write(command)

            self.log('Command issued to', term.node)

            jobId = jobInfo_.name
            node = term.node
            proc = term

        elif self._dispatchMode == 'local':
            command = '{setenv}darun.py {workspace} {jobName}'.format(
                setenv = setenv,
                workspace = self._workspace,
                jobName = jobInfo_.name
            )

            self.log(command)

            proc = subprocess.Popen(command,
                                    shell = True,
                                    stdin = PIPE,
                                    stdout = open(logdir + '/' + jobInfo_.name + '.log', 'w'),
                                    stderr = subprocess.STDOUT
                                )

            self.log('Subprocess started')

            jobId = jobInfo_.name
            node = 'localhost'

        with self._lock:
            jobInfo_.id = jobId
            jobInfo_.state = 'PENDING'
            jobInfo_.proc = proc
            jobInfo_.node = node

    def dispatch(self, subMax_, bsubOptions = '', setenv = '', logdir = ''):
        lastUpdateTime = time.time()
        
        while True:
            with self._lock:
                nReady = len(self._readyJobs)
                nRunning = len(self._runningJobs)
            
            if nReady != 0 and nRunning < subMax_:
                self.submitOne(bsubOptions, setenv, logdir)
                continue

            if nReady == 0 and nRunning == 0:
                break
                
            self._update.wait(20.)
            self._update.clear()

            self.printStatus()                

            if time.time() - lastUpdateTime > 20.:
                self.queryJobStates()
                self.printStatusWeb()
                lastUpdateTime = time.time()

    def queryJobStates(self):

        exited = []
        def setExited(jobInfo):
            jobInfo.state = 'EXITED'
            self._runningJobs.remove(jobInfo)
            if self._resubmit: self._readyJobs.append(jobInfo)
            exited.append(jobInfo)
        
        try:
            if self._dispatchMode == 'bsub':
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
                with self._lock:
                    for jobInfo in self._jobInfo.values():
                        if jobInfo.id in lsfTracked:
                            idx = lsfTracked.index(jobInfo.id)
                            jobInfo.node = lsfNodes[idx]
                        elif jobInfo in self._runningJobs:
                            setExited(jobInfo)

            elif self._dispatchMode == 'lxplus':
                with self._lock:
                    for jobInfo in filter(lambda jobInfo : not jobInfo.proc.isOpen(), self._jobInfo.values()):
                        if jobInfo in self._runningJobs:
                            setExited(jobInfo)
    
            elif self._dispatchMode == 'local':
                with self._lock:
                    for jobInfo in filter(lambda jobInfo : jobInfo.proc.poll() is not None, self._jobInfo.values()):
                        if jobInfo in self._runningJobs:
                            setExited(jobInfo)

        except:
            self.log('Job status query failed')

        for jobInfo in exited:
            self.log('Set state', jobInfo.name, 'EXITED')
            with open(self._workspace + '/logs/' + jobInfo.name + '.fail', 'w') as failLog:
                pass

    def countJobs(self):
        jobCounts = dict((key, 0) for key in DADispatcher.states)

        with self._lock:
            for jobInfo in self._jobInfo.values():
                jobCounts[jobInfo.state] += 1

        return jobCounts

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

    def printStatusWeb(self, copyLogs = False):
        if copyLogs:
            for fileName in os.listdir(self._workspace + '/logs'):
                sourceName = self._workspace + '/logs/' + fileName
                destName = HTMLDIR + '/logs/' + fileName
                if not os.path.exists(destName) or os.stat(sourceName).st_mtime > os.stat(destName).st_mtime:
                    with open(sourceName, 'r') as source:
                        with open(destName, 'w') as dest:
                            for line in source:
                                dest.write(line)

        statusStr = self.printStatus(asstr = True)

        allJobs = self._jobInfo.keys()
        try:
            allJobs.sort(lambda x, y : int(x) - int(y))
        except ValueError:
            allJobs.sort()
       
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

            with self._lock:
                for jobName in allJobs:
                    jobInfo = self._jobInfo[jobName]
                    htmlFile.write('<tr><td><a href="logs/{name}.log">{name}</a></td><td>{jobId}</td><td>{node}</td><td>{state}</td></tr>\n'.format(name = jobInfo.name, jobId = jobInfo.id, node = jobInfo.node, state = jobInfo.state))

            htmlFile.write('''
</table>
</body>
</html>
''')
        
                
class DownloadRequestServer(DAServer):
    """
    Service for download traffic control. Serializes multiple requests to a single resource (disk).
    Protocol:
      SRV: send READY
      CLT: trasfer, send OK or FAIL (no action on server side in either case)
    """

    MAXDEPTH = 4

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

        with self._lock:
            while True:
                try:
                    request = self._queue.get(block = False) # will raise Queue.Empty exception when empty
                    while True:
                        response = request.recv(1024)
                        if response == 'DONE': break
                        request.send('GO')
                except:
                    break


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
        
        self._outputNumber = 0

        self.inputQueue = Queue.Queue()

        self.workdir = workdir_

        if not self.workdir:
            while True:
                self.workdir = TMPDIR + '/' + string.join(random.sample(string.ascii_lowercase, 6), '')
                try:
                    os.mkdir(self.workdir)
                except OSError:
                    pass

        try:
            os.mkdir(self.workdir + '/input')
        except OSError:
            if not os.path.isdir(self.workdir + '/input'):
                raise RuntimeError("Reducer input directory")
        
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
            Hadder.ROOT.gSystem.Load("libTreePlayer.so")
            Hadder.ROOT = ROOT
            sys.argv = argv # sys.argv is read when a first call to ROOT object is made
        
        Reducer.__init__(self, name_, targetFileName_, maxSize_, workdir_)

        self._lock = threading.Lock()

    def reduce(self):
        with self._lock:
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
        self.close(force = True)

    def open(self):
        if self.isOpen(): return
        
        self._session = subprocess.Popen(['ssh', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet', '-T', self._servName],
            stdin = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT,
	    preexec_fn = lambda : signal.signal(signal.SIGINT, signal.SIG_IGN))
        
        self.node = self.communicate('echo $HOSTNAME')[0]
        print 'Terminal opened on ' + self.node
        
    def close(self, force = False):
        if not self.isOpen(): return
        
        try:
            if force: self._session.terminate()
            else: self._session.stdin.write('exit\n')

            iTry = 0
            while iTry < 5 and self._session.poll() is None:
                time.sleep(1)
                iTry += 1

            if self._session.poll() is None:
                self._session.terminate()
                
            self.node = ''
        except OSError:
            pass
        except:
            print 'Failed to close SSH connection:', sys.exc_info()[0:2]

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
    inputOpts.add_option("-n", "--files-per-job", type="int", dest='filesPerJob', help='Number of files per job.', metavar="NUM", default=8)
    inputOpts.add_option("-f", "--file-format", dest="nameFormat",
        help="""\
        Wildcard expression of the name of the files in the dataset to use. Multiple files (separated by comma) can be related through the wildcard character.
        Each instance of the match is passed to the worker function. Example: susyEvents*.root,susyTriggers*.root""",
        default="*.root", metavar="FORMAT")
    parser.add_option_group(inputOpts)

    runtimeOpts = OptionGroup(parser, "Runtime options", "These options can be changed for each job submission.")
    runtimeOpts.add_option('-c', '--client', dest='client', help="Client to use for processing. Options are bsub (lxbatch), lxplus, and local.", default='bsub', metavar="MODE")
    runtimeOpts.add_option("-b", "--bsub-options", dest="bsubOptions", help='Options to pass to bsub command. -J and -cwd are set automatically. Example: -R "rusage[pool=2048]" -q 8nh', metavar="OPTIONS", default="-q 8nh")
    runtimeOpts.add_option("-D", "--debug", action="store_true", dest="debug", help="")
    runtimeOpts.add_option("-r", "--resubmit", action="store_true", dest="resubmit", help="Resubmit the job")
    runtimeOpts.add_option("-R", "--recover", action="store_true", dest="recover", help="Recover failed jobs")
    runtimeOpts.add_option("-P", "--max-parallel", dest="maxParallel", help="Maximum (approximate) number of parallel jobs to run.", type="int", default=25, metavar="NUM")
    runtimeOpts.add_option("-j", "--jobs", dest="jobs", help="Jobs to submit.", default="", metavar="JOB1[,JOB2[,JOB3...]]")
    runtimeOpts.add_option("-M", "--max-jobs", dest="maxJobs", help="Maximum number of jobs to submit.", type="int", default=-1, metavar="NUM")
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

        jobConfig["setenv"] = options.setenv.strip().rstrip(';')
        if jobConfig['setenv']:
            jobConfig['setenv'] += '; '

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

    tcpServer.setLog(HTMLDIR + '/logs/server.log')

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

    ### OPEN TERMINAL ###

    terminal = Terminal(TERMNODE)

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

        try:
            os.mkdir(tmpWorkspace + '/reduce')
        except OSError:
            pass

        reducer = eval(jobConfig['reducer'])('reduce', jobConfig['outputFile'], jobConfig['maxSize'], tmpWorkspace + '/reduce')
        reducer.setLog(tcpServer.log)
#        tcpServer.addService(reducer)
# Choosing to run reducer "offline" and not as a service; adds stability with a price of little time rag after the jobs are done. darun jobs will upload the output to $TMPDIR/{key}/reduce

    # dispatcher
    dispatcher = DADispatcher(workspace, options.client, resubmit = options.autoResubmit, terminal = terminal)

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

    dispatcher.dispatch(options.maxParallel, bsubOptions = options.bsubOptions, setenv = jobConfig['setenv'], logdir = HTMLDIR + '/logs')

    dispatcher.printStatus()
    dispatcher.printStatusWeb()

    print ''

    if dispatcher.countJobs()['DONE'] != len(allJobs):
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
            sys.exit(1)

    ### COPY LOG FILES ###

    for logFile in os.listdir(HTMLDIR + '/logs'):
        shutil.copy(HTMLDIR + '/logs/' + logFile, workspace + '/logs/' + logFile)

    ### CLEANUP ###

    tcpServer.stop()
    
    terminal.close()
            
    shutil.rmtree(tmpWorkspace, ignore_errors = True)

    print 'Done.'
