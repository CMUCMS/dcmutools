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

srcdir = os.path.dirname(os.path.realpath(__file__))
if srcdir not in sys.path:
    sys.path.append(srcdir)
from globals import *
from reducer import *
from terminal import Terminal
from dscp import dscp

SERVERONLY = False # for debugging purpose


class DelegatingTCPServer(SocketServer.ThreadingTCPServer):
    """
    Threading TCP server with list of services to be provided. Each client authenticates with its id-key pair
    during the initial handshake.
    The implementation does not follow the original design concept of SocketServer in that no RequestHandlerClass is used.
    This is to eliminate the communication cost between the server and the handler object.
    """

    def __init__(self):
        if DEBUG: print 'Initializing ThreadingTCPServer'

        port = MINPORT
        while True:
            try:
                SocketServer.ThreadingTCPServer.__init__(self, ('', port), None)
                break
            except socket.error:
                if port == MAXPORT:
                    raise
                port += 1

        self._logLines = Queue.Queue()
        self._log = sys.stdout
        self._services = {}
        self._running = False

    def setLog(self, logFileName_):
        self._log = open(logFileName_, 'w', 0)

    def verify_request(self, request_, clientAddr_):
        return 'dispatch' in self._services

    def process_request_thread(self, request_, clientAddr_):
        jobName = 'unknown'
        serviceName = 'unknown'
        try:
            jobName, key = request_.recv(1024).split()

            if not self._services['dispatch'].authenticate(jobName, key):
                raise RuntimeError('Wrong id-key pair: ' + str((jobName, key)))
                
            request_.send('SVC')
            serviceName = request_.recv(1024)
            
            self.log('Job: ' + jobName + ' Service: ' + serviceName)
            
            service = self._services[serviceName]
            
            vcode = service.canServe(jobName)
            if vcode == 1:
                request_.send('ACCEPT')

                # now delegate the actual task to service
                if DEBUG: self.log('Passing ' + jobName + ' to ' + serviceName)
                service.serve(request_, jobName)

            elif vcode == 0:
                request_.send('WAIT')

            else:
                raise RuntimeError(serviceName + ' cannot serve ' + jobName)
        except:
            try:
                clientHost = socket.gethostbyaddr(clientAddr_[0])[0]
            except:
                clientHost = 'unknown'
                
            self.log('Exception processing request from ' + clientHost + ': ' + str(sys.exc_info()[0:2]))
            try:
                request_.send('REJECT')
            except:
                if DEBUG: self.log('Failed to send REJECT ' + str(sys.exc_info()[0:2]))
                pass

        # let the client close the connection first; frees up server from having to stay in TIME_WAIT
        try:
            request_.recv(1024)
        except:
            pass
        
        self.close_request(request_)
        self.log('Closed request from ' + jobName + ' for ' + serviceName)

    def start(self):
        if 'dispatch' not in self._services:
            raise RuntimeError('Server cannot start without dispatcher')
        
        thread = threading.Thread(target = self.serve_forever, name = 'tcpServer')
        thread.daemon = True
        thread.start()
        self._running = True

        self._logThread = threading.Thread(target = self._writeLog, name = 'serverLog')
        self._logThread.daemon = True
        self._logThread.start()

        if DEBUG: print 'started TCP server'

    def stop(self):
        self.log('Stopping server..')
        if self._running: self.shutdown()
        self.server_close()
        self.log('Server stopped.')
        self._running = False
        self._logLines.put('EOF')
        self._logThread.join()

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
            if line == 'EOF':
                if self._log != sys.stdout: self._log.close()
                break

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

    STATES = [
        'UNKNOWN',
        'CREATED',
        'PENDING',
        'RUNNING',
        'DONE',
        'FAILED',
        'EXITED'
    ]

    CLUSTERS = ['lsf', 'interactive', 'local']
    FALLBACK = {'lsf': 'interactive', 'interactive': '', 'local': ''}
    MAXACTIVE = {'lsf': 40, 'interactive': 10, 'local': 20}

    class JobInfo(object):
        """
        Struct for job information.
        """
        
        def __init__(self, name_):
            self.name = name_
            self.key = string.join(random.sample(string.ascii_lowercase, 4), '')
            self.cluster = ''
            self.proc = None
            self.node = ''
            self.state = 'CREATED'
            self.lastHB = 0

                
    def __init__(self, workspace_, resubmit = False, terminal = None):
        DAServer.__init__(self, 'dispatch')
        
        self._workspace = workspace_
        self._webDir = ''
        self._jobInfo = {}
        self._resubmit = resubmit
        self._readyJobs = dict([(k, []) for k in DADispatcher.CLUSTERS])
        self._activeJobs = dict([(k, []) for k in DADispatcher.CLUSTERS])
        self._lock = threading.Lock()
        self._stateChanged = threading.Event()
        if terminal:
            self._terminal = terminal
        else:
            self._terminal = Terminal(TERMNODE, verbose = True)

        self.options = dict([(k, '') for k in DADispatcher.CLUSTERS])

    def __del__(self):
        for cluster in DADispatcher.CLUSTERS:
            for jobInfo in self._activeJobs[cluster]:
                self.kill(jobInfo)

    def authenticate(self, jobName_, key_):
        try:
            return key_ == self._jobInfo[jobName_].key
        except:
            return False

    def canServe(self, jobName_):
        if jobName_ in self._jobInfo: return 1
        else: return -1

    def serve(self, request_, jobName_):
        jobInfo = self._jobInfo[jobName_]

        jobInfo.lastHB = time.time()

        try:
            response = request_.recv(1024)
            request_.send('OK')
            if response == 'HB':
                self.log('Heart beat from', jobName_)
                return
            else:
                self.log('Set state', jobName_, response)
        except:
            response = 'UNKNOWN'

        with self._lock:
            try:
                jobInfo.state = response

                finished = False
                
                if jobInfo.state == 'DONE':
                    self._activeJobs[jobInfo.cluster].remove(jobInfo)
                    finished = True
                elif jobInfo.state == 'FAILED':
                    self._activeJobs[jobInfo.cluster].remove(jobInfo)
                    if self._resubmit:
                        self._readyJobs[jobInfo.cluster].append(jobInfo)

                    finished = True

                if finished:
                    if jobInfo.cluster == 'interactive':
                        jobInfo.proc.close()
                    elif jobInfo.cluster == 'local':
                        jobInfo.proc.communicate()

            except:
                self.log('Exception while serving', jobName_, sys.exc_info()[0:2])

        if jobInfo.state == 'FAILED':
            with open(self._workspace + '/logs/' + jobName_ + '.fail', 'w') as failLog:
                pass

        self._stateChanged.set()
        
    def createJob(self, jobName_, cluster_, append = True):
        jobInfo = DADispatcher.JobInfo(jobName_)
        jobInfo.cluster = cluster_
        self._jobInfo[jobName_] = jobInfo
        if append: self._readyJobs[cluster_].append(jobInfo)
        if DEBUG: self.log('Created', jobName_)
        return jobInfo

    def submitOne(self, cluster, logdir = ''):
        if len(self._activeJobs[cluster]) >= DADispatcher.MAXACTIVE[cluster] or \
           len(self._readyJobs[cluster]) == 0:
            return False

        with self._lock:
            try:
                jobInfo = self._readyJobs[cluster].pop(0)
            except IndexError:
                return False
       
        if DEBUG: self.log('submit', jobInfo.name)

        if self.submit(jobInfo, logdir):
            with self._lock:
                self._activeJobs[cluster].append(jobInfo)
            return True
        else:
            with self._lock:
                self._readyJobs[cluster].append(jobInfo)
            return False

    def submit(self, jobInfo_, logdir = ''):
        self.log('Submitting job ', jobInfo_.name)

        if not logdir:
            logdir = self._workspace + '/logs'

        try:
            if jobInfo_.cluster == 'lsf':
                command = "bsub -J {jobName} -o {log} -cwd '$TMPDIR' {options} 'source {environment};darun.py {workspace} {jobName} {key}'".format(
                    jobName = jobInfo_.name,
                    log = logdir + '/' + jobInfo_.name + '.log',
                    options = self.options['lsf'],
                    environment = self._workspace + '/environment',
                    workspace = self._workspace,
                    key = jobInfo_.key
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
                    raise Exception
    
                self.log('lxbatch job ID for {0} is {1}'.format(jobInfo_.name, matches.group(1)))
    
                proc = matches.group(1)
                node = ''
    
            elif jobInfo_.cluster == 'interactive':
                command = 'cd $TMPDIR;source {environment};darun.py {workspace} {jobName} {key} >> {log} 2>&1;exit'.format(
                    environment = self._workspace + '/environment',
                    workspace = self._workspace,
                    jobName = jobInfo_.name,
                    key = jobInfo_.key,
                    log = logdir + '/' + jobInfo_.name + '.log'
                )
    
                self.log(command)
    
                term = Terminal(TERMNODE)
                term.write(command)
    
                self.log('Command issued to', term.node)
    
                proc = term
                node = term.node
    
            elif jobInfo_.cluster == 'local':
                command = 'cd {tmpdir};source {environment};darun.py {workspace} {jobName} {key}'.format(
                    tmpdir = TMPDIR,
                    environment = self._workspace + '/environment',
                    workspace = self._workspace,
                    jobName = jobInfo_.name,
                    key = jobInfo_.key
                )
    
                self.log(command)
    
                proc = subprocess.Popen(command,
                                        shell = True,
                                        stdin = subprocess.PIPE,
                                        stdout = subprocess.PIPE,
                                        stderr = subprocess.STDOUT
                                    ) # stdout will be redirected to a log file within the job
    
                self.log('Subprocess started')
    
                node = 'localhost'

        except:
            return False
            
        jobInfo_.proc = proc
        jobInfo_.state = 'PENDING'
        jobInfo_.node = node
        jobInfo_.lastHB = time.time()

        self._stateChanged.set()

        return True

    def kill(self, jobInfo_):
        if jobInfo_.cluster == 'lsf':
            response = self._terminal.communicate('bkill {0}'.format(jobInfo_.proc))
            for line in response:
                self.log(line)

        elif jobInfo_.cluster == 'interactive':
            if jobInfo_.proc.isOpen():
                jobInfo_.proc.close(force = True)

        elif jobInfo_.cluster == 'local':
            if jobInfo_.proc.poll() is None:
                jobInfo_.proc.terminate()

        try:
            self._jobInfo.pop(jobInfo_.name)
        except:
            self.log('Exception while trying to remove', jobInfo_.name)

    def dispatch(self, logdir = ''):
        monitorTerminate = threading.Event()
        monitorThread = threading.Thread(target = self.monitor, args = (monitorTerminate,), name = 'monitor')
        monitorThread.daemon = True
        monitorThread.start()

        while True:
            submitted = False
            for cluster in DADispatcher.CLUSTERS:
                if self.submitOne(cluster, logdir):
                    submitted = True

            if submitted: continue

            with self._lock:
                nReady = reduce(lambda x, y : x + y, map(len, self._readyJobs.values()))
                nActive = reduce(lambda x, y : x + y, map(len, self._activeJobs.values()))

            if nReady == 0 and nActive == 0:
                break

            self._stateChanged.wait(60.)

            if not self._stateChanged.isSet(): # timeout
                exited = []
                noHB = []
                if len(self._activeJobs['lsf']) != 0:
                    lsfNodes = {}
                    try:
                        response = self._terminal.communicate('bjobs')[1:]
                        if DEBUG: print 'bjobs', response
            
                        for line in response:
                            id = line.split()[0]
                            node = line.split()[5]
                            lsfNodes[id] = node
        
                    except:
                        self.log('Job status query failed')                    
                
                    with self._lock:
                        for jobInfo in self._activeJobs['lsf']:
                            # Two different tasks - if id is in the list of ids, set the node name.
                            # If not, the job may have exited abnormally - check state.
                            if jobInfo.proc in lsfNodes:
                                if not jobInfo.node:
                                    jobInfo.node = lsfNodes[jobInfo.proc]

                                if jobInfo.lastHB < time.time() - 120:
                                    noHB.append(jobInfo)
                            else:
                                exited.append(jobInfo)

                with self._lock:
                    for jobInfo in self._activeJobs['interactive']:
                        if not jobInfo.proc.isOpen():
                            exited.append(jobInfo)
                        elif jobInfo.lastHB < time.time() - 120:
                            noHB.append(jobInfo)
            
                    for jobInfo in self._activeJobs['local']:
                        if jobInfo.proc.poll() is not None:
                            exited.append(jobInfo)
                        elif jobInfo.lastHB < time.time() - 120:
                            noHB.append(jobInfo)

                if self._resubmit:
                    for jobInfo in noHB:
                        fallback = DADispatcher.FALLBACK[jobInfo.cluster]
                        if fallback and len(self._activeJobs[fallback]) < DADispatcher.MAXACTIVE[fallback]:
                            # This job is not responding and there is a space in the fallback queue
                            exited.append(jobInfo)
                
                if len(exited):
                    with self._lock:
                        for jobInfo in exited:
                            self.log('Set state', jobInfo.name, 'EXITED')
                            jobInfo.state = 'EXITED'
                            self._activeJobs[jobInfo.cluster].remove(jobInfo)
            
                            if self._resubmit:
                                self.kill(jobInfo) # removes from self._jobInfo
                                cluster = jobInfo.cluster
                                fallback = DADispatcher.FALLBACK[cluster]
                                if fallback and len(self._activeJobs[fallback]) < DADispatcher.MAXACTIVE[fallback]:
                                    self.log('Submission of', jobInfo.name, 'falling back to', fallback)
                                    cluster = fallback
                                    
                                newJobInfo = self.createJob(jobInfo.name, cluster, append = False)
                                if self.submit(newJobInfo, logdir):
                                    self._activeJobs[cluster].append(newJobInfo)
                                else:
                                    self._readyJobs[cluster].append(newJobInfo)

                                self._stateChanged.set()
            
                    for jobInfo in exited:
                        with open(self._workspace + '/logs/' + jobInfo.name + '.fail', 'w') as failLog:
                            pass

            time.sleep(1) # allow the monitor thread to catch up
            self._stateChanged.clear()

        monitorTerminate.set()
        self._stateChanged.set()
        monitorThread.join()

    def monitor(self, _terminate):
        self.printStatus()
        self.printStatusWeb()

        nCycle = 0
        while True:
            self._stateChanged.wait()
            if _terminate.isSet():
                break

            self.printStatus()
            nCycle += 1
            if nCycle == 20:
                self.printStatusWeb()
                nCycle = 0

    def countJobs(self):
        jobCounts = dict((key, 0) for key in DADispatcher.STATES)

        with self._lock:
            for jobInfo in self._jobInfo.values():
                jobCounts[jobInfo.state] += 1

        return jobCounts

    def setWebDir(self, dir_):
        self._webDir = dir_
        try:
            os.mkdir(self._webDir)
        except OSError:
            pass

    def printStatus(self):
        jobCounts = self.countJobs()

        line = ''
        for state in DADispatcher.STATES:
            line += ' {state}: {n}'.format(state = state, n = jobCounts[state])

        line = '\r' + line
        line += ' ' * 10
        sys.stdout.write(line)
        if DEBUG: sys.stdout.write('\n')
        sys.stdout.flush()

    def printStatusWeb(self, copyLogs = False):
        if not self._webDir: return
        
        if copyLogs:
            logDir = self._webDir + '/logs'
            if not os.path.exists(logDir):
                os.mkdir(logDir)

            for fileName in os.listdir(self._workspace + '/logs'):
                sourceName = self._workspace + '/logs/' + fileName
                destName = logDir + '/' + fileName
                if not os.path.exists(destName) or os.stat(sourceName).st_mtime > os.stat(destName).st_mtime:
                    shutil.copy(sourceName, destName)

        allJobs = self._jobInfo.keys()

        summaryName = self._webDir + '/summary.dat'
        if not os.path.exists(summaryName):
            with open(summaryName, 'w') as summaryFile:
                # add more info in the future?
                summaryFile.write('workspace = ' + self._workspace)

        statusDir = self._webDir + '/status'

        if not os.path.exists(statusDir):
            os.makedirs(statusDir)
            for job in allJobs:
                with open(statusDir + '/' + job + '.UNKNOWN', 'w') as log:
                    pass

        with self._lock:
            for statusFile in os.listdir(statusDir):
                jobName = statusFile[:statusFile.rfind('.')]
                if jobName not in self._jobInfo: continue
                current = statusFile[statusFile.rfind('.') + 1:]
                actual = self._jobInfo[jobName].state
                if current != actual:
                    os.rename(statusDir + '/' + statusFile, statusDir + '/' + jobName + '.' + actual)


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


class ReduceServer(DAServer):
    """
    DAServer interface to reducer
    """

    def __init__(self, name_, reducer_):
        DAServer.__init__(self, name_)
        self._reducer = reducer_
        self._reducer.setLogFunc(self.log)

    def canServe(self, jobName_):
        return 1

    def serve(self, request_, jobName_):
        try:
            request_.recv(1024)
            request_.send(self._reducer.workdir + '/input')
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
        self._reducer.inputQueue.put(response)

        self._reducer.reduce()


class DownloadRequestServer(QueuedServer):
    """
    Service for download traffic control. Serializes multiple requests to a single resource (disk).
    Protocol:
      SRV: send READY
      CLT: trasfer, send OK or FAIL (no action on server side in either case)
    """

    MAXDEPTH = 6

    def _serveOne(self, request_, jobName_):
        request_.recv(1024)
        request_.send('OK')
        request_.recv(1024) # job responds when done downloading
        

class DSCPServer(QueuedServer):
    """
    Service for handling LFN creation. Second argument is the path to the working directory.
    The directory must exist if a non-empty string is passed.
    """

    MAXDEPTH = 0

    def __init__(self, name_, targetLFDir_, workdir = ''):
        QueuedServer.__init__(self, name_)

        self._targetDir = targetLFDir_
        self.workdir = workdir

        if not self.workdir:
            while True:
                self.workdir = TMPDIR + '/' + string.join(random.sample(string.ascii_lowercase, 6), '')
                try:
                    os.mkdir(self.workdir)
                    break
                except OSError:
                    pass

        if DEBUG: self.log('Working directory', self.workdir)

    def _serveOne(self, request_, jobName_):
        while True:
            fileName = request_.recv(1024)

            if fileName == 'DONE':
                request_.send('OK')
                break

            if not os.path.exists(fileName):
                request_.send('FAILED')
                self.log(fileName, 'does not exist')
                raise RuntimeError('NoFile')

            try:
                success = dscp(fileName, self._targetDir + '/' + os.path.basename(fileName), self.log)
                if success:
                    os.remove(fileName)
                else:
                    self.log(fileName, 'was not copied to', self._targetDir)
                    raise Exception
            except:
                request_.send('FAILED')
                self.log('Error in copying', fileName)
                raise RuntimeError('CopyError')

            request_.send('OK')


if __name__ == '__main__':

    for pid in [pid for pid in os.listdir('/proc') if pid.isdigit()]:
        try:
            with open(os.path.join('/proc', pid, 'cmdline', 'rb')) as procInfo:
                if 'dap.py' in procInfo.read():
                    print 'dap already running with pid', pid
                    raise EnvironmentalError
        except IOError:
            pass

    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage = "usage: %prog [options] dataset [dataset2 [dataset3 ...]] macro")

    execOpts = OptionGroup(parser, "Job execution options", "These options will be saved in the job configuration and will be used at each job execution.")
    execOpts.add_option("-w", "--workspace", dest = 'workspace', help = 'Name of the job directory', default = "", metavar = "DIR")
    execOpts.add_option("-e", "--environment", dest = 'environment', help = 'List of commands to set up the job environment. Defaults to the output of "scram runtime -sh" in CMSSW_BASE.', default = DEFAULTENV, metavar = "CMD")
    execOpts.add_option("-a", "--analyzer-arguments", dest = 'analyzerArguments', help = "Arguments to be passed to the initialize() function of the analyzer object.", default = "", metavar = "ARGS")
    execOpts.add_option("-I", "--include", dest = "includePaths", help = "Include path for compilation", default = "", metavar = "-IDIR1 [-IDIR2 [-IDIR3 ...]]")
    execOpts.add_option("-l", "--lib", dest = "libraries", help = "Libraries to load", default = "", metavar = "LIB1[,LIB2[,LIB3...]]")
    execOpts.add_option("-d", "--output-dir", dest = "outputDir", help = "Output [host:]directory", default = "./", metavar = "DIR")
    execOpts.add_option("-u", "--reducer", dest = "reducer", help = "Reducer module. Set to None to disable reducer. Default is Hadder.", default = "Hadder", metavar = "MODULE")
    execOpts.add_option("-o", "--output-file", dest = "outputFile", help = "Output file name. Ignored when reducer is None", default = "", metavar = "OUT")
    execOpts.add_option("-m", "--max-size", dest = "maxSize", help = "Approximate maximum size in MB of the reducer output.", default = 1024, metavar = "SIZE")
    parser.add_option_group(execOpts)

    inputOpts = OptionGroup(parser, "Input options", "These options are used at job creation time to configure the input.")
    inputOpts.add_option("-n", "--files-per-job", type = "int", dest = 'filesPerJob', help = 'Number of files per job.', metavar = "NUM", default = 8)
    inputOpts.add_option("-f", "--file-format", dest = "nameFormat",
        help = """\
        Wildcard expression of the name of the files in the dataset to use. Multiple files (separated by comma) can be related through the wildcard character.
        Each instance of the match is passed to the worker function. Example: susyEvents*.root,susyTriggers*.root""",
        default = "*.root", metavar = "FORMAT")
    parser.add_option_group(inputOpts)

    runtimeOpts  =  OptionGroup(parser, "Runtime options", "These options can be changed for each job submission.")
    runtimeOpts.add_option('-c', '--cluster', dest = 'cluster', help = "Default cluster to use for processing. Options are lsf, interactive, and local.", default = 'lsf', metavar = "MODE")
    runtimeOpts.add_option('-T', '--terminal', dest = 'terminal', help = "Terminal node to use.", default = TERMNODE, metavar = 'NODE')
    runtimeOpts.add_option("-b", "--bsub-options", dest = "bsubOptions", help = 'Options to pass to bsub command. -J and -cwd are set automatically. Example: -R "rusage[pool = 2048]" -q 8nh', metavar = "OPTIONS", default = "-q 8nh")
    runtimeOpts.add_option("-N", "--no-cleanup", action = "store_true", dest = "noCleanup", help = "")
    runtimeOpts.add_option("-D", "--debug", action = "store_true", dest = "debug", help = "")
    runtimeOpts.add_option("-r", "--resubmit", action = "store_true", dest = "resubmit", help = "Resubmit the job")
    runtimeOpts.add_option("-R", "--recover", action = "store_true", dest = "recover", help = "Recover failed jobs")
    runtimeOpts.add_option("-j", "--jobs", dest = "jobs", help = "Jobs to submit.", default = "", metavar = "JOB1[,JOB2[,JOB3...]]")
    runtimeOpts.add_option("-M", "--max-jobs", dest = "maxJobs", help = "Maximum number of jobs to submit.", type = "int", default = -1, metavar = "NUM")
    runtimeOpts.add_option("-S", "--auto-resubmit", action = "store_true", dest = "autoResubmit", help = "Automatically resubmit failed jobs")
    runtimeOpts.add_option("-t", "--no-submit", action = "store_true", dest = "noSubmit", help = "Compile and quit")
    parser.add_option_group(runtimeOpts)

    (options, args) = parser.parse_args()

    if options.cluster not in DADispatcher.CLUSTERS:
        raise RuntimeError('Cluster ' + options.cluster + ' not supported')

    sys.argv = sys.argv[0:1]

    DEBUG = options.debug
    
    resubmit = options.resubmit or options.recover

    if not resubmit and (len(args) < 2 or not options.workspace):
        parser.print_usage()
        sys.exit(1)

    ### CREATE SERVER ###

    taskID = string.join(random.sample(string.ascii_lowercase, 4), '')

    tcpServer = DelegatingTCPServer()

    ### OPEN TERMINAL ###

    if DEBUG: print "opening terminal"

    terminal = Terminal(options.terminal, verbose = True)

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

        ### OPEN WORKSPACE ###

        os.mkdir(workspace)
        os.mkdir(workspace + '/inputs')
        os.mkdir(workspace + '/logs')

        if options.environment.strip():
            cmds = terminal.communicate(options.environment.strip())
            with open(workspace + '/environment', 'w') as envFile:
                for cmd in cmds:
                    envFile.write(cmd + '\n')


    print 'Using {0} as workspace'.format(workspace)

    ### RUNTIME-SPECIFIC CONFIGURATIONS ###

    jobConfig['taskID'] = taskID
    jobConfig['serverHost'] = os.environ['HOSTNAME']
    jobConfig['serverPort'] = tcpServer.server_address[1]
    jobConfig['serverWorkDir'] = TMPDIR + '/' + taskID
    jobConfig["logDir"] = HTMLDIR + '/' + taskID + '/logs'
    # In principle log directory can be anywhere; we are choosing it to be directly in the HTMLDIR for convenience

    ### SAVE JOB CONFIGURATION ###

    with open(workspace + '/jobconfig.py', 'w') as configFile:
        configFile.write('jobConfig = ' + str(jobConfig))

    ### LOG DIRECTORY ###

    if os.path.realpath(jobConfig['logDir']) != workspace + '/logs':
        os.makedirs(jobConfig['logDir'])

    ### SERVER LOG ###

    tcpServer.setLog(jobConfig['logDir'] + '/server.log')

    ### JOB LIST ###
    
    if DEBUG: print "preparing the lists of input files"

    if options.recover:
        allJobs = map(lambda name: os.path.basename(name).replace(".fail", ""), glob.glob(workspace + "/logs/*.fail"))
    elif options.resubmit:
        allJobs = map(os.path.basename, glob.glob(workspace + '/inputs/*'))
    else:
        # Create lists from directory entries
        # Keep the list of disks at the same time - will launch as many DownloadRequestServers as there are disks later
        
        if not options.nameFormat:
            raise RuntimeError("Input file name format")

        # Get mount point -> device mapping
        mountMap = {}
        with open('/proc/mounts') as mountData:
            for line in mountData:
                mountMap[line.split()[1]] = line.split()[0]

        datasets = args[0:len(args) - 1]
        
        nameSpecs = map(str.strip, options.nameFormat.split(','))

        lfnList = []
        for spec in nameSpecs:
            paths = []
            for dataset in datasets:
                paths += glob.glob(dataset + '/' + spec)
            paths.sort()

            if len(lfnList) == 0:
                for path in paths:
                    lfnList.append([path])
            elif len(lfnList) == len(paths):
                for iP in range(len(paths)):
                    lfnList[iP].append(paths[iP])
            else:
                raise RuntimeError('Number of input files do not match')

        diskNames = set()

        allJobs = []
        pfnList = []
        
        for lfnRow in lfnList:
            pfnRow = []
            for lfn in lfnRow:
                # Convert to physical file names and write to input list along with device name
                if os.path.islink(lfn):
                    pfn = os.readlink(lfn)
                else:
                    pfn = lfn

                dirName = os.path.dirname(pfn)
                while dirName not in mountMap: dirName = os.path.dirname(dirName)
                pfnRow.append((mountMap[dirName], pfn))
                diskNames.add(mountMap[dirName])

            if len(pfnList) == 0:
                allJobs.append(str(len(allJobs)))

            pfnList.append(pfnRow)

            if len(pfnList) == options.filesPerJob:
                currentJob = allJobs[-1]
                with open(workspace + '/inputs/' + currentJob, 'w') as inputList:
                    for pfnRow in pfnList:
                        inputList.write(str(pfnRow) + '\n')

                pfnList = []

        if len(pfnList):
            currentJob = allJobs[-1]
            with open(workspace + '/inputs/' + currentJob, 'w') as inputList:
                for pfnRow in pfnList:
                    inputList.write(str(pfnRow) + '\n')

        with open(workspace + '/disks', 'w') as diskList:
            diskList.write(str(diskNames) + '\n')

    # Created job list

    # Filter jobs if requested
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

    # lock file
    os.chmod(jobConfig['macro'], 0444)
    
    terminal.communicate('cd ' + workspace + ';source environment;python macro.py > logs/compile.log 2>&1')

    with open(workspace + '/logs/compile.log', 'r') as logFile:
        for line in logFile:
            if 'Error' in line or 'fail' in line:
                os.chmod(jobConfig['macro'], 0644)
                raise RuntimeError("Compilation failed")

    ### TEMPORARY SPACE FOR MODULES ###
            
    tmpWorkspace = TMPDIR + '/' + taskID
    os.mkdir(tmpWorkspace)

    ### SERVICES ###

    with open(workspace + '/disks') as diskList:
        diskNames = eval(diskList.readline())
        
    # download request handlers
    for diskName in diskNames:
        tcpServer.addService(DownloadRequestServer(diskName))

    # reducers
    reducer = None
    if jobConfig['reducer'] != 'None':
        if not jobConfig['outputFile']:
            raise RuntimeError('Reducer requires output file name specification')

        os.mkdir(tmpWorkspace + '/reduce')

        reducer = eval(jobConfig['reducer'])(jobConfig['outputFile'], maxSize = jobConfig['maxSize'], workdir = tmpWorkspace + '/reduce')
        # reduceServer = ReduceServer('reduce', reducer)
        # reduceServer.setLog(tcpServer.log)
        # tcpServer.addService(reduceServer)
        # Choosing to run reducer "offline" and not as a service; adds stability with a price of little time rag after the jobs are done. darun jobs will upload the output to $TMPDIR/{taskID}/reduce
        reducer.setLog(lambda *args : tcpServer.log(string.join(map(str, args)), name = 'reducer'))

    if jobConfig['outputNode'] == os.environ['HOSTNAME'] and jobConfig['outputDir'][0:7] == '/store/':
        # The output is an LFN on this storage element.
        # If reducer is ON, DSCP will be executed from Reducer.copyOutputTo().
        os.mkdir(tmpWorkspace + '/dscp')
        tcpServer.addService(DSCPServer('dscp', jobConfig['outputDir'], workdir = tmpWorkspace + '/dscp'))

    # dispatcher
    dispatcher = DADispatcher(workspace, resubmit = options.autoResubmit, terminal = terminal)
    dispatcher.setWebDir(HTMLDIR + '/' + taskID)
    dispatcher.options['lsf'] = options.bsubOptions

    for jobName in allJobs:
        dispatcher.createJob(jobName, options.cluster)

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

    try:
        dispatcher.dispatch(logdir = jobConfig['logDir'])
    except KeyboardInterrupt:
        pass

    dispatcher.printStatus()
    dispatcher.printStatusWeb()

    print ''

    completed = True

    if dispatcher.countJobs()['DONE'] != len(allJobs):
        print 'Some jobs are not in DONE state.'
        completed = False

    ### REDUCER ###

    if completed and reducer:
        print 'Reducing output'
        
        for outputFile in glob.glob(reducer.workdir + '/input/*'):
            reducer.inputQueue.put(os.path.basename(outputFile))

        reducer.reduce()
        reducer.finalize()

        dest = jobConfig['outputDir']
        if jobConfig['outputNode'] != os.environ['HOSTNAME']:
            dest = jobConfig['outputNode'] + ':' + dest

        if reducer.copyOutputTo(dest):
            if len(reducer.succeeded) != len(allJobs):
                print 'Number of input files to reducer does not match the number of jobs: {0}/{1}'.format(len(reducer.succeeded), len(allJobs))
                completed = False
            elif len(reducer.failed):
                print 'Final reduction failed. Output in', reducer.workdir
                completed = False
            else:
                reducer.cleanup()
        else:
            completed = False
            print 'Copy to ' + jobConfig['outputNode'] + ':' + jobConfig['outputDir'] + ' failed.'

    ### COPY LOG FILES ###

    if os.path.realpath(jobConfig['logDir']) != workspace + '/logs':
        for logFile in os.listdir(jobConfig['logDir']):
            shutil.copy(jobConfig['logDir'] + '/' + logFile, workspace + '/logs/' + logFile)

    ### CLEANUP ###

    tcpServer.stop()
    
    terminal.close()

    if not completed and not options.noCleanup:
        response = ''
        while response != 'Y' and response != 'n':
            print 'Clean workdir (' + tmpWorkspace + ')? [Y/n] (n):'
            response = sys.stdin.readline().strip()
            if not response: response = 'n'

        if response == 'Y': completed = True
            
    if completed and not options.noCleanup: shutil.rmtree(tmpWorkspace, ignore_errors = True)

    os.chmod(jobConfig['macro'], 0644)

    print 'Done.'
