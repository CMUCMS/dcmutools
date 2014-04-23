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
from daserver import *
from reducer import *
from terminal import Terminal
from dscp import dscp

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

        port = MINPORT
        while True:
            try:
                SocketServer.ThreadingTCPServer.__init__(self, ('', port), None)
                break
            except socket.error:
                if port == MAXPORT:
                    raise
                port += 1

        self._key = key_
        self._logLines = Queue.Queue()
        self._log = sys.stdout
        self._services = {}
        self._running = False

    def setLog(self, logFileName_):
        self._log = open(logFileName_, 'w', 0)

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
                if DEBUG: self.log('Failed to send REJECT ' + str(sys.exc_info()[0:2]))
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
                    if DEBUG: self.log('Passing ' + jobName + ' to ' + serviceName)
                    service.serve(request_, jobName)
                except:
                    if DEBUG: self.log('Service exception ' + str(sys.exc_info()[0:2]))
                    try:
                        request_.send('REJECT')
                    except:
                        if DEBUG: self.log('Failed to send REJECT ' + str(sys.exc_info()[0:2]))
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
                if DEBUG: self.log('Failed to send REJECT ' + str(sys.exc_info()[0:2]))
                pass

        self.close_request(request_)
        self.log('Closed request from ' + jobName + ' for ' + serviceName)

    def start(self):
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
        self._webDir = ''
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
            request_.send('OK')
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

    def submitOne(self, bsubOptions = '', logdir = ''):
        if len(self._readyJobs):
            jobInfo = self._readyJobs[0]
            self.submit(jobInfo, bsubOptions, logdir)

    def submit(self, jobInfo_, bsubOptions = '', logdir = ''):
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
            command = "bsub -J {jobName} -o {log} -cwd '$TMPDIR' {options} 'source {environment};darun.py {workspace} {jobName}'".format(
                jobName = jobInfo_.name,
                log = logdir + '/' + jobInfo_.name + '.log',
                options = bsubOptions,
                environment = self._workspace + '/environment',
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
            command = 'cd $TMPDIR;source {environment};darun.py {workspace} {jobName} > {log} 2>&1;exit'.format(
                environment = self._workspace + '/environment',
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
            command = 'cd {tmpdir};source {environment};darun.py {workspace} {jobName}'.format(
                tmpdir = TMPDIR,
                environment = self._workspace + '/environment',
                workspace = self._workspace,
                jobName = jobInfo_.name
            )

            self.log(command)

            proc = subprocess.Popen(command,
                                    shell = True,
                                    stdin = subprocess.PIPE,
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.STDOUT
                                ) # stdout will be redirected to a log file within the job

            self.log('Subprocess started')

            jobId = jobInfo_.name
            node = 'localhost'

        with self._lock:
            jobInfo_.id = jobId
            jobInfo_.state = 'PENDING'
            jobInfo_.proc = proc
            jobInfo_.node = node

    def dispatch(self, subMax_, bsubOptions = '', logdir = ''):
        lastUpdateTime = time.time()
        
        while True:
            with self._lock:
                nReady = len(self._readyJobs)
                nRunning = len(self._runningJobs)
            
            if nReady != 0 and nRunning < subMax_:
                self.submitOne(bsubOptions, logdir)
                continue

            if nReady == 0 and nRunning == 0:
                break
                
            self._update.wait(20.)
            self._update.clear()

            self.printStatus()                

            if time.time() - lastUpdateTime > 60.:
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

                response = self._terminal.communicate('bjobs')[1:]
                if DEBUG: print 'bjobs', response
    
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

    def setWebDir(self, dir_):
        self._webDir = dir_
        try:
            os.mkdir(self._webDir)
        except OSError:
            pass

    def printStatus(self):
        jobCounts = self.countJobs()

        line = ''
        for state in DADispatcher.states:
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
                current = statusFile[statusFile.rfind('.') + 1:]
                actual = self._jobInfo[jobName].state
                if current != actual:
                    os.rename(statusDir + '/' + statusFile, statusDir + '/' + jobName + '.' + actual)

                
class DownloadRequestServer(QueuedServer):
    """
    Service for download traffic control. Serializes multiple requests to a single resource (disk).
    Protocol:
      SRV: send READY
      CLT: trasfer, send OK or FAIL (no action on server side in either case)
    """

    MAXDEPTH = 6

    def _serveOne(self, request_, jobName_):
        while True:
            response = request_.recv(1024)
            request_.send('OK')
            if response == 'DONE': break
        

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
    runtimeOpts.add_option('-c', '--client', dest = 'client', help = "Client to use for processing. Options are bsub (lxbatch), lxplus, and local.", default = 'bsub', metavar = "MODE")
    runtimeOpts.add_option("-b", "--bsub-options", dest = "bsubOptions", help = 'Options to pass to bsub command. -J and -cwd are set automatically. Example: -R "rusage[pool = 2048]" -q 8nh', metavar = "OPTIONS", default = "-q 8nh")
    runtimeOpts.add_option("-D", "--debug", action = "store_true", dest = "debug", help = "")
    runtimeOpts.add_option("-r", "--resubmit", action = "store_true", dest = "resubmit", help = "Resubmit the job")
    runtimeOpts.add_option("-R", "--recover", action = "store_true", dest = "recover", help = "Recover failed jobs")
    runtimeOpts.add_option("-P", "--max-parallel", dest = "maxParallel", help = "Maximum (approximate) number of parallel jobs to run.", type = "int", default = 25, metavar = "NUM")
    runtimeOpts.add_option("-j", "--jobs", dest = "jobs", help = "Jobs to submit.", default = "", metavar = "JOB1[,JOB2[,JOB3...]]")
    runtimeOpts.add_option("-M", "--max-jobs", dest = "maxJobs", help = "Maximum number of jobs to submit.", type = "int", default = -1, metavar = "NUM")
    runtimeOpts.add_option("-S", "--auto-resubmit", action = "store_true", dest = "autoResubmit", help = "Automatically resubmit failed jobs")
    runtimeOpts.add_option("-t", "--no-submit", action = "store_true", dest = "noSubmit", help = "Compile and quit")
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

    ### OPEN TERMINAL ###

    if DEBUG: print "opening terminal"

    terminal = Terminal(TERMNODE)

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

    jobConfig['key'] = jobKey
    jobConfig['serverHost'] = os.environ['HOSTNAME']
    jobConfig['serverPort'] = tcpServer.server_address[1]
    jobConfig['serverWorkDir'] = TMPDIR + '/' + jobKey
    jobConfig["logDir"] = HTMLDIR + '/' + jobKey + '/logs'
    # In principle log directory can be anywhere; we are choosing it to be directly in the HTMLDIR for convenience

    ### SAVE JOB CONFIGURATION ###

    with open(workspace + '/jobconfig.py', 'w') as configFile:
        configFile.write('jobConfig = ' + str(jobConfig))

    tmpWorkspace = TMPDIR + '/' + jobKey
    os.mkdir(tmpWorkspace)

    ### LOG DIRECTORY ###

    for logFile in glob.glob(workspace + '/logs/*.log'):
        os.remove(logFile)

    if os.path.realpath(jobConfig['logDir']) != workspace + '/logs':
        os.makedirs(jobConfig['logDir'])

    ### SERVER LOG ###

    tcpServer.setLog(jobConfig['logDir'] + '/server.log')

    ### JOB LIST ###
    
    if DEBUG: print "preparing the lists of input files"

    if options.resubmit:
        allJobs = map(os.path.basename, glob.glob(workspace + '/inputs/*'))
    elif options.recover:
        allJobs = map(lambda name: os.path.basename(name).replace(".fail", ""), glob.glob(workspace + "/logs/*.fail"))
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
    
    terminal.communicate('cd ' + workspace + ';source environment;python macro.py > logs/compile.log 2>&1')

    with open(workspace + '/logs/compile.log', 'r') as logFile:
        for line in logFile:
            if 'Error' in line or 'fail' in line:
                raise RuntimeError("Compilation failed")

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

        reducer = eval(jobConfig['reducer'])('reduce', jobConfig['outputFile'], maxSize = jobConfig['maxSize'], workdir = tmpWorkspace + '/reduce')
        reducer.setLog(tcpServer.log)
        # tcpServer.addService(reducer)
        # Choosing to run reducer "offline" and not as a service; adds stability with a price of little time rag after the jobs are done. darun jobs will upload the output to $TMPDIR/{key}/reduce

    if jobConfig['outputNode'] == os.environ['HOSTNAME'] and jobConfig['outputDir'][0:7] == '/store/':
        # The output is an LFN on this storage element.
        # If reducer is ON, DSCP will be executed from Reducer.copyOutputTo().
        os.mkdir(tmpWorkspace + '/dscp')
        tcpServer.addService(DSCPServer('dscp', jobConfig['outputDir'], workdir = tmpWorkspace + '/dscp'))

    # dispatcher
    dispatcher = DADispatcher(workspace, options.client, resubmit = options.autoResubmit, terminal = terminal)
    dispatcher.setWebDir(HTMLDIR + '/' + jobKey)

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

    try:
        dispatcher.dispatch(options.maxParallel, bsubOptions = options.bsubOptions, logdir = jobConfig['logDir'])
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
            elif len(reducer.failed):
                print 'Final reduction failed. Output in', reducer.workdir
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
            
    if completed: shutil.rmtree(tmpWorkspace, ignore_errors = True)

    print 'Done.'
