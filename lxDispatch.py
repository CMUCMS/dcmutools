#!/usr/bin/env python

import threading
import os
import sys
import copy
import glob
import re
import time
import signal
import shutil
import string
import SocketServer
import subprocess

import Queue

TIMEOUT = 30
MINPORT = 40000
MAXPORT = 40009
MAXCONN = 200
DCMUHOST = "dcmu00"
LXPLUS = "lxplus"
CMSSW_BASE = "/afs/cern.ch/user/y/yiiyama/cmssw/SLC6Ntuplizer5314"
USER = os.environ['USER']

DEBUG = False

CREATED = 0
SUBMITTED = 1
RUNNING = 2
DONE = 3
FAILED = 4
SUCCLOGGED = 5
FAILLOGGED = 6

SSH = 'exec ssh -oStrictHostKeyChecking=no'
SCP = 'exec scp -oStrictHostKeyChecking=no'

def filterJobs(jobList_, mask_ = (CREATED, SUBMITTED, RUNNING, DONE, FAILED, SUCCLOGGED, FAILLOGGED)):
    try:
        mask_[0]
    except TypeError:
        mask_ = tuple([mask_])
    except:
        return {}

    return dict(filter(lambda item: item[1][1] in mask_, jobList_.items()))

def countJobs(jobList_, mask_ = (CREATED, SUBMITTED, RUNNING, DONE, FAILED, SUCCLOGGED, FAILLOGGED)):
    try:
        mask_[0]
    except TypeError:
        mask_ = tuple([mask_])
    except:
        return 0
    
    return len(filter(lambda item: item[1][1] in mask_, jobList_.items()))

def ignoreSIGINT():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

class JobListManager:
    """
    Store and manipulate the list of jobs. Each job list is a dictionary that maps a job name to its (id, status).
    A pair of job list and accompanying lock is kept in self._jobLists, whose index is the job group name.
    """
    
    def __init__(self):
	self._jobLists = {}
	self._maxLen = 0
        self._displayCount = 0
        self._lock = threading.Lock()

    def addList(self, name_, list_):
        if DEBUG: print 'adding list ' + name_
        self._lock.acquire()
	self._jobLists[name_] = (list_, threading.Lock())
        self._lock.release()

    def disactivate(self, name_):
        if DEBUG: print 'disactivating ' + name_
        self._lock.acquire()
        self._jobLists[name_] = (self._jobLists[name_][0], None)
        self._lock.release()

    def getList(self, name_):
        try:
            return self._jobLists[name_][0]
        except KeyError:
            return {}

    def getLock(self, name_):
        self._lock.acquire()
        try:
            return self._jobLists[name_][1]
        except KeyError:
            return None
        finally:
            self._lock.release()

    def getActiveJobGroups(self):
        self._lock.acquire()
        jobGroups = filter(lambda k: self._jobLists[k][1] is not None, self._jobLists.keys())
        self._lock.release()

        return jobGroups

    def getIdNameMapping(self):
        mapping = {}

        jobGroups = self.getActiveJobGroups()

        for jobGroup in jobGroups:
            jobList, lock = self._jobLists[jobGroup]

            lock.acquire()
            try:
                for jobName, idStatus in jobList.items():
                    if idStatus[0] != 0 and idStatus[1] not in (SUCCLOGGED, FAILLOGGED): mapping[idStatus[0]] = (jobGroup, jobName)
            finally:
                lock.release()

        return mapping

    def updateDisplay(self):
	if DEBUG: sys.stdout.write('\n')

        jobGroups = sorted(self._jobLists.keys())

        if self._displayCount == 0:
            line = ''
            for jobGroup in jobGroups:
                line += jobGroup + ' '

            sys.stdout.write(line + '\n')
	
        line = '\r'
    
        for jobGroup in jobGroups:
            jobList, lock = self._jobLists[jobGroup]

	    nJobs = len(jobList)

            if lock is not None: lock.acquire()
            try:
                nRunning = countJobs(jobList, RUNNING)
                nSucceeded = countJobs(jobList, (DONE, SUCCLOGGED))
                nFailed = countJobs(jobList, (FAILED, FAILLOGGED))
            finally:
                if lock is not None: lock.release()

            nPending = nJobs - nRunning - nSucceeded - nFailed

            line += '[' + str(nSucceeded + nFailed)
            if nFailed != 0: line += '({0})'.format(nFailed)
            line += '/' + str(nJobs)
            if nPending: line += '({0})'.format(nPending)
            line += '] '

	if len(line) > self._maxLen:
	    self._maxLen = len(line)
	else:
	    sys.stdout.write('\r' + ' ' * self._maxLen)
	    
        sys.stdout.write(line)
        sys.stdout.flush()
    
        if DEBUG:
            sys.stdout.write('\n')
            sys.stdout.flush()

        self._displayCount += 1

class DispatchServer:
    """
    Master class that sets up the job workspace, starts the network server, and dispatches the jobs.
    """

    class DownloadRequestHandler(SocketServer.BaseRequestHandler):
        """
        handle() sorts the download request connections from individual jobs to appropriate threads
        """

        def handle(self):
            try:
                jobName = self.request.recv(1024).strip()
            except:
                if DEBUG: print 'communication error during handshake', sys.exc_info()
                self.request.close()
                return

            if DEBUG: print 'handling request from job {0}'.format(jobName)

            jobLists = self.server.jobLists
            for jobGroup in jobLists.getActiveJobGroups():
                if jobName in jobLists.getList(jobGroup):
                    self.server.queues[jobGroup].put((jobName, self.request))
                    break
            else:
                if DEBUG: print 'job {0} not found in any list'.format(jobName)
                self.request.send('notfound')
                self.request.close()


    class DownloadRequestServer(SocketServer.TCPServer):
        """
        TCP server with additional data members
        """

        def __init__(self, addr_):
            self.jobLists = JobListManager()
            self.queues = {}

            self.timeout = TIMEOUT

            SocketServer.TCPServer.__init__(self, addr_, DispatchServer.DownloadRequestHandler)

        def __del__(self):
            for jobGroup in self.queues.keys():
                self.deregisterJobGroup(jobGroup)

        def registerJobGroup(self, jobGroup_):
            self.queues[jobGroup_] = Queue.Queue()

        def deregisterJobGroup(self, jobGroup_):
            try:
                queue = self.queues[jobGroup_]
                while not queue.empty():
                    jobName, sock = queue.get()
                    sock.close()

                self.queues.pop(jobGroup_)
            except KeyError:
                pass

        def verify_request(self, request_, clientAddr_):
            try:
                if reduce(lambda x, y: x + y, map(Queue.Queue.qsize, self.queues.values())) > MAXCONN:
                    if DEBUG: print 'Request queues are full. Denying access from ', clientAddr_
                    raise RuntimeError('Too many connections in queue')
                else:
                    return True
            except:
                return False

        def process_request(self, request_, clientAddr_):
            """
            Overriding SocketServer.TCPServer method to avoid closing the socket
            """
            self.finish_request(request_, clientAddr_)


    class Reducer:
        """
        Wrapper class for the reducer command specified in the job configuration.
        The command will be invoked with the final destination directory name and the log file name as the last two arguments.
        Upon start, the command should immediately return a string
        $HOSTNAME:$WORKDIR
        representing the working directory of the reducer process.
        The executable is then expected to receive from stdin a string of form
        JOBGROUP FILENAME
        for each file to be reduced. Empty FILENAME will signify the end of the list of file for the JOBGROUP.
        An empty line is passed when all files have been processed.
        The excutable must respond to stdout after each input. The response can be a blank line, or a one-line message
        in case of an error.
        """

        def __init__(self, cmd_, outputDir_, workspace_):
    
            if DEBUG: print 'checking reduceCmd'
    
            self._cmd = cmd_
            self._outputDir = outputDir_
            self._workspace = workspace_
    
            class Alarm(Exception):
                pass
    
            def alarmHndl(num, frame):
                raise Alarm
    
            defHndl = signal.signal(signal.SIGALRM, alarmHndl)
    
            logFile = self._workspace + '/logs/reduce.log'
            command = "{cmd} {dir} {log}".format(cmd = self._cmd, dir = self._outputDir, log = logFile)
            if DEBUG: print command
    
            proc = subprocess.Popen(command, shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

            signal.alarm(5)
            try:
                response = proc.stdout.readline().strip()
                if DEBUG: print response
                matches = re.match('([a-zA-Z0-9_.-]+)[:]([a-zA-Z0-9_/.-]+)', response)
                while matches is None:
                    response = proc.stdout.readline().strip()
                    if DEBUG: print response
                    matches = re.match('([a-zA-Z0-9_.-]+)[:]([a-zA-Z0-9_/.-]+)', response)

                proc.stdin.write('\n')
                while proc.poll() is None: pass
                
                signal.alarm(0)
            except Alarm:
                print 'ReduceCmd did not respond to EOF'
                raise
            finally:
                if proc.poll() is None: proc.terminate()
    
            os.remove(logFile)
    
            signal.signal(signal.SIGALRM, defHndl)
    
            if DEBUG: print 'reducer function confirmed. Starting on SSH'
    
            self._proc = subprocess.Popen("{ssh} {host} '{command}'".format(ssh = SSH, host = LXPLUS, command = command), shell = True,
                stdout = subprocess.PIPE, stdin = subprocess.PIPE, stderr = subprocess.STDOUT,
                preexec_fn = ignoreSIGINT)

            matches = re.match('([a-zA-Z0-9_.-]+)[:]([a-zA-Z0-9_/.-]+)', self._proc.stdout.readline().strip())
            while matches is None:
                matches = re.match('([a-zA-Z0-9_.-]+)[:]([a-zA-Z0-9_/.-]+)', self._proc.stdout.readline().strip())
    
            self._host = matches.group(1)
            self._tmpDir = matches.group(2)
            if DEBUG: print 'reducer workarea {0}:{1}'.format(self._host, self._tmpDir)

            self._flag = threading.Event()
            thread = threading.Thread(target = self.monitorOutput, name = 'reducerMonitor')
            thread.daemon = True
            thread.start()
    
        def getWorkspace(self):
            return self._host + ':' + self._tmpDir
    
        def reduce(self, jobGroup_, file_):
            self._proc.stdin.write(jobGroup_ + ' ' + file_ + '\n')
    
        def close(self, jobGroup_ = ''):
            if not jobGroup_: self._flag.set()
            if self._proc.poll() is None:
                self._proc.stdin.write(jobGroup_ + '\n')

        def monitorOutput(self):
            while not self._flag.isSet():
                if self._proc.poll() is not None: break
                response = self._proc.stdout.readline().strip()
                if response:
                    print "Reducer message: " + response


    class Terminal:
        """
        A wrapper for an ssh session.
        """

        def __init__(self):
            
            self._term = None
            self._lock = threading.Lock()
            self.open()

        def __del__(self):

            self.close()

        def open(self):

            if self._term and self._term.poll() is None: return
            self._term = subprocess.Popen("{ssh} -T {host}".format(ssh = SSH, host = LXPLUS), shell = True,
                stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT,
                preexec_fn = ignoreSIGINT)
            response = self.communicate('echo $HOSTNAME')
            print 'Terminal opened on ' + response[0]
            
        def close(self, force_ = False):

            if not self._term or self._term.poll() is not None: return
            try:
                self._lock.acquire()
                if force_: self._term.terminate()
                else: self._term.stdin.write('exit\n')
                self._lock.release()
                while self._term.poll() is None: pass
            except:
                print 'Failed to close SSH connection'

        def write(self, line_):
            
            self._lock.acquire()
            try:
                self._term.stdin.write(line_.strip() + '\n')
            except:
                print 'Failed to write {0} to terminal'.format(line_.strip()), sys.exc_info()
                self.close(True)
                self.open()
            finally:
                self._lock.release()

        def read(self):

            self._lock.acquire()
            response = ''
            try:
                response = self._term.stdout.readline().strip()
            except:
                print 'Failed to read from terminal', sys.exc_info()
                self.close(True)
                self.open()
            finally:
                self._lock.release()

            return response

        def communicate(self, inputs_):

            output = []
            if DEBUG: print 'communicate: ', inputs_
            self._lock.acquire()
            try:
                if isinstance(inputs_, list):
                    for line in inputs_:
                        self._term.stdin.write(line.strip() + '\n')
                elif isinstance(inputs_, str):
                    self._term.stdin.write(inputs_.strip() + '\n')

                self._term.stdin.write('echo EOL\n')

                while True:
                    line = self._term.stdout.readline().strip()
                    if line == 'EOL': break
                    output.append(line)
            except:
                print 'Communication with terminal failed: ', sys.exc_info()
                self.close(True)
                self.open()
            finally:
                self._lock.release()

            return output

    def __init__(self, config_):
        """
        Workspace setup:
        [workspace]/inputs
                   /logs
                   /jobconfig.py
        If workspace already exists, config_ will be overridden by the job configuration saved in jobConfig.py.
        After the workspace has been set up, the function performs the following checks:
         - ROOT macro integrity
         - Server port availability
        """
        
        # create job directories
        self._workspace = config_["workspace"]

        if os.path.exists(self._workspace):
            sys.path.append(self._workspace)
            import jobconfig
            config_ = copy.copy(jobconfig.jobConfig)
        else:
            os.mkdir(self._workspace)
     
            os.mkdir(self._workspace + '/inputs')
            os.mkdir(self._workspace + '/logs')

        self._outputDir = config_["outputDir"]
    
        # check source
        macro = config_["macro"]
        print 'Checking ' + macro
        if os.path.exists(macro):
            with open(macro) as macroFile:
                for line in macroFile:
                    if re.match('^[ ]*class[ ]+' + config_["analyzer"], line): break
                else:
                    raise RuntimeError("analyzer not found in macro")
        else:
            raise RuntimeError("Macro not found")
    
        # check port availability and start server
        port = MINPORT
        while port <= MAXPORT:
            try:
                self._reqServer = DispatchServer.DownloadRequestServer(('', port))
                break
            except:
                port += 1
        else:
            raise RuntimeError("Allowed ports all in use")

        # set setenv command
        if config_['setenv']:
            self._setenv = config_['setenv'] + ';'
        else:
            self._setenv = ''

        # setup reducer
        if config_["reduceCmd"]:
            self._reducer = DispatchServer.Reducer(config_["reduceCmd"], self._outputDir, self._workspace)
            print 'Reducer started at ' + self._reducer.getWorkspace()
        else:
            self._reducer = None

        self._updateFlag = threading.Event()
        self._exitFlag = threading.Event()

        self._jobThreads = []

        # open terminal
        self._terminal = DispatchServer.Terminal()
    
        # compile the source in the terminal
        if DEBUG: print "writing compile code"

        with open(self._workspace + '/macro.py', 'w') as configFile:
            configFile.write('import sys\n')
            configFile.write('import ROOT\n')
	    configFile.write('ROOT.gROOT.SetBatch()\n')
            configFile.write('rootlogon = ROOT.gEnv.GetValue("Rint.Logon", "")\n')
            configFile.write('if rootlogon:\n')
            configFile.write('    ROOT.gROOT.Macro(rootlogon)\n')
            for lib in config_["libraries"]:
                configFile.write('ROOT.gSystem.Load("' + lib + '")\n')
            configFile.write('ROOT.gSystem.AddIncludePath("' + config_["includePaths"] + '")\n')
            configFile.write('if ROOT.gROOT.LoadMacro("' + macro + '+") != 0: sys.exit(1)\n')

            configFile.write('arguments = (')
            for arg in config_['analyzerArguments'].split(','):
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

        with open(self._workspace + '/jobconfig.py', 'w') as configFile:
	    configFile.write('jobConfig = ' + str(config_))
    
        response = self._terminal.communicate(self._setenv + 'cd ' + self._workspace + ';python macro.py > logs/compile.log 2>&1')

        with open(self._workspace + '/logs/compile.log', 'r') as logFile:
            for line in logFile:
                if 'Error' in line or 'fail' in line:
                    raise RuntimeError("Compilation failed")

        if DEBUG: print 'exiting server Ctor'


    def start(self):
 
        thread = threading.Thread(target = self._reqServer.serve_forever, name = 'reqServer')
        thread.daemon = True
        thread.start()
        if DEBUG: print 'started request server'


    def runThread(self, threadName_, jobList_, bsubOptions_, autoResubmit_, maxJobs_):

        queue = Queue.Queue()
        self._reqServer.jobLists.addList(threadName_, jobList_)
        self._reqServer.registerJobGroup(threadName_)
    
        thread = threading.Thread(target = self.lxDispatch, args = (threadName_, bsubOptions_, autoResubmit_, maxJobs_), name = threadName_)
        thread.daemon = True
        thread.start()
        if DEBUG: print 'started thread ' + threadName_

        self._jobThreads.append(thread)


    def monitorJobs(self):

        self._reqServer.jobLists.updateDisplay()

        try:
            loops = 0
            while len(self._jobThreads):
                thread = self._jobThreads[0]
                if DEBUG: print 'checking thread ' + thread.name
                thread.join(TIMEOUT)

                if not thread.isAlive():
                    self._jobThreads.remove(thread)
                    self._reqServer.jobLists.disactivate(thread.name)
                    self._reqServer.deregisterJobGroup(thread.name)
                    continue

                if self._updateFlag.isSet():
                    self._reqServer.jobLists.updateDisplay()
                    self._updateFlag.clear()

                loops += 1
                if loops % 3 == 1:
                    try:
                        self.monitorLSFStatus()
                    except:
                        print 'Exception in monitorLSFStatus', sys.exc_info()

            if DEBUG: print 'all threads finished.'

            if self._reducer: self._reducer.close()

            while self.monitorLSFStatus() != 0: time.sleep(30)

        except:
            print "\nInterrupted. exiting.."

            self._exitFlag.set()
            for thread in self._jobThreads:
                thread.join()
                print 'Thread ' + thread.name + ' terminated'

            if DEBUG: print 'all job threads joined'

            self._terminal.close()
            if self._reducer: self._reducer.close()

            raise
        
        finally:
            self._reqServer.shutdown()            


    def setLogStatus(self, jobName_, status_):
    
        currentName = self._workspace + "/logs/" + jobName_ + '.*'
        logs = glob.glob(currentName)
    
        if len(logs) == 0 and jobName_ != '*':
            currentName = currentName.replace('*', 'run')
            open(currentName, 'w').close()
            logs = [currentName]
    
        for logName in logs:
            if 'run' not in logName: continue
            newName = logName[0:logName.rfind('.') + 1] + status_
            if newName != logName:
                os.rename(logName, newName)


    def lxDispatch(self, jobGroup_, bsubOptions_, autoResubmit_, maxJobs_):

        jobList = self._reqServer.jobLists.getList(jobGroup_)
        lock = self._reqServer.jobLists.getLock(jobGroup_)

        thread = threading.Thread(target = self.bsubClient, args = (bsubOptions_, jobList, maxJobs_, lock), name = 'bsub-' + jobGroup_)
        thread.daemon = True
        thread.start()

        nJobs = len(jobList)
    
        while True:
            if self._exitFlag.isSet():
                self.bkill(jobList, lock)
                break
    
            lock.acquire()
            nFin = countJobs(jobList, (DONE, FAILED, SUCCLOGGED, FAILLOGGED))
            lock.release()
            
            if autoResubmit_:
                lock.acquire()
                failedJobs = filterJobs(jobList, (FAILED, FAILLOGGED))
                lock.release()
                if len(failedJobs):
                    self.bsubClient(bsubOptions_, failedJobs, 0)
                    lock.acquire()
                    for jobName in failedJobs.keys():
                        jobList[jobName] = failedJobs[jobName]
                    lock.release()
    
                nFin -= len(failedJobs)
    
            if nFin == nJobs:
                if DEBUG: print '{0}: All jobs finished. exiting'.format(jobGroup_)
                break
           
            try:
                jobName, sock = self._reqServer.queues[jobGroup_].get(True, TIMEOUT)
            except Queue.Empty:
                if DEBUG: print 'request poll timeout: ' + jobGroup_
                continue
    
            if DEBUG: print 'received request from {0} at {1}'.format(jobName, sock.getsockname()[1])
    
            try:
                sock.send('ready')
                response = sock.recv(1024)
                sock.send('OK')

                if response == 'done' or response == 'fail':
                    if response == 'done' and self._reducer:
                        files = []
                        while True:
                            output = sock.recv(1024).strip()
                            sock.send('OK')
                            if not output: break
                            if ':' in output: output = output[output.find(':') + 1:]
                            files.append(output)

                        for file in files:
                            self._reducer.reduce(jobGroup_, file)
                            if DEBUG: print 'passing {0} to reducer'.format(file)
                    
                    if DEBUG: print 'setting status of {0} to {1}'.format(jobName, response)

                    status = DONE if response == 'done' else FAILED

                    lock.acquire()
                    jobList[jobName] = (jobList[jobName][0], status)
                    lock.release()
       
                    self._updateFlag.set()
            except:
                print '\n{0}: communication with {1} failed'.format(jobGroup_, jobName), sys.exc_info()
            finally:
                if DEBUG: print 'closing connection to {0} at {1}'.format(jobName, sock.getsockname()[1])
                sock.close()

        if self._reducer:
            self._reducer.close(jobGroup_)

        if DEBUG: print 'lxDispatch {0} returning'.format(jobGroup_)

    def bsubClient(self, bsubOptions_, jobList_, maxJobs_, lock_ = None):

        for jobName in jobList_.keys():
            if lock_ and maxJobs_:
                while True:
                    lock_.acquire()
                    nSubmitted = countJobs(jobList_, (SUBMITTED, RUNNING))
                    lock_.release()
                    if nSubmitted < maxJobs_: break
                    time.sleep(TIMEOUT)

            for nAttempt in range(10):
                if self._exitFlag.isSet():
                    return
    
                if DEBUG: print 'submitting job {0}, trial {1}'.format(jobName, nAttempt)

                logFile = self._workspace + '/logs/' + jobName + '.run'
                open(logFile, 'w').close()

                command = "bsub {options} -J {jobName} -o {log} '{setenv}lxClient.py {workspace} {jobName} {port} {dest}'".format(options = bsubOptions_, jobName = jobName, log = logFile, setenv = self._setenv, workspace = self._workspace, port = self._reqServer.server_address[1], dest = self._outputDir if not self._reducer else self._reducer.getWorkspace())

                bsubout = self._terminal.communicate(command)
                
                if len(bsubout) == 0 or 'submitted' not in bsubout[0]:
                    if DEBUG: print 'bsub failed'
                    continue
    
                matches = re.search('<([0-9]+)>', bsubout[0])
                if not matches:
                    if DEBUG: print 'bsub returned ' + bsubout[0]
                    continue
     
                if DEBUG: print 'lxbatch job ID for {0} is {1}'.format(jobName, matches.group(1))

                if lock_: lock_.acquire()
                jobList_[jobName] = (int(matches.group(1)), SUBMITTED)
                if lock_: lock_.release()
                
                self._updateFlag.set()
                break
    
            else:
                print '\n' + jobName + " failed to submit."

                if lock_: lock_.acquire()
                jobList_[jobName] = (0, FAILED)
                if lock_: lock_.release()
                
                self._updateFlag.set()
    
        if DEBUG: print 'submitted {0} jobs'.format(len(jobList_))


    def bkill(self, jobList_, lock_ = None):
    
        lock_.acquire()
        killList = filterJobs(jobList_, (SUBMITTED, RUNNING))
        lock_.release()
    
        for jobName, idStatus in killList.items():
            if DEBUG: print 'killing job {0}'.format(jobName)

            self._terminal.write('bkill {0}'.format(idStatus[0]))
    
            if lock_: lock_.acquire()
            jobList_[jobName] = (jobList_[jobName][0], FAILED)
            if lock_: lock_.release()
        
        self._updateFlag.set()


    def monitorLSFStatus(self):
        # get all un-logged jobs
        idToJob = self._reqServer.jobLists.getIdNameMapping()

        if DEBUG: print 'jobs with valid LSF ID:', idToJob

        nRunning = 0
    
        if len(idToJob):
            if DEBUG: print "querying LSF status"

            jobLines = self._terminal.communicate('bjobs -a')

            for line in jobLines[1:]:
                words = line.split()

                try:
                    bjobId = int(words[0])
                except ValueError:
                    continue

                if bjobId in idToJob:
                    jobGroup = idToJob[bjobId][0]
                    jobName = idToJob[bjobId][1]

                    jobList = self._reqServer.jobLists.getList(jobGroup)
                    lock = self._reqServer.jobLists.getLock(jobGroup)

                    statusWord = words[2]
                    status = SUBMITTED
                    log = ''
                    if statusWord == 'RUN':
                        status = RUNNING
                        nRunning += 1
                    elif statusWord == 'DONE':
                        status = SUCCLOGGED
                        log = 'done'
                    elif statusWord == 'EXIT':
                        status = FAILLOGGED
                        log = 'fail'

                    lock.acquire()
                    if jobList[jobName][1] < status:
                        jobList[jobName] = (bjobId, status)
                        if DEBUG: print "set job {0} status to {1}".format(jobName, statusWord)
                    lock.release()

                    if log: self.setLogStatus(jobName, log)

                    idToJob.pop(bjobId)
            
            # jobs still in the list are completely lost
            for jobId, jobSpec in idToJob.items():
                jobGroup = jobSpec[0]
                jobName = jobSpec[1]

                jobList = self._reqServer.jobLists.getList(jobGroup)
                lock = self._reqServer.jobLists.getLock(jobGroup)
                
                lock.acquire()
                jobList[jobName] = (jobId, FAILLOGGED)
                lock.release()
                
                self.setLogStatus(jobName, 'fail')

            self._updateFlag.set()

        return nRunning


if __name__ == '__main__':

    try:
        if DCMUHOST not in os.environ['HOSTNAME']:
            raise EnvironmentError
    except:
        print "This is not ", DCMUHOST
        raise

    try:
        if 'bash' not in os.environ['SHELL']:
            raise EnvironmentError
    except:
        print "Only bash supported"
	raise

    SETENV = 'cd ' + CMSSW_BASE + ';eval `scramv1 runtime -sh`'

    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage="usage: %prog [options] dataset [dataset2 [dataset3 ...]] macro")

    execOpts = OptionGroup(parser, "Job execution options", "These options will be saved in the job configuration and will be used at each job execution.")
    execOpts.add_option("-w", "--workspace", dest='workspace', help='Name of the job directory', default="", metavar="DIR")
    execOpts.add_option("-e", "--setenv", dest='setenv', help='Command to issue before running the job', default=SETENV, metavar="CMD")
    execOpts.add_option("-a", "--analyzer-arguments", dest='analyzerArguments', help="Arguments to be passed to the initialize() function of the analyzer object.", default="", metavar="ARGS")
    execOpts.add_option("-I", "--include", dest="includePaths", help="Include path for compilation", default="", metavar="-IDIR1 [-IDIR2 [-IDIR3 ...]]")
    execOpts.add_option("-l", "--lib", dest="libraries", help="Libraries to load", default="", metavar="LIB1[,LIB2[,LIB3...]]")
    execOpts.add_option("-o", "--output-dir", dest="outputDir", help="Output directory", default=".", metavar="OUT")
    execOpts.add_option("-m", "--reduceCmd", dest="reduceCmd", help="Reducer command. The command will be invoked with the output directory and log file names as the last two arguments. The executable is expected to parse the input file names from stdin and return the path of the reduced file on returning. The end of input is signaled by a blank line.", default="", metavar="EXECUTABLE")
    parser.add_option_group(execOpts)

    inputOpts = OptionGroup(parser, "Input options", "These options are used at job creation time to configure the input.")
    inputOpts.add_option("-n", "--files-per-job", type="int", dest='filesPerJob', help='Number of files per job', metavar="NUM", default=1)
    #    inputOpts.add_option("-x", "--external-list", dest='externalList', help="External list of strings to be used for job scheduling")
    inputOpts.add_option("-f", "--file-format", dest="nameFormat",
        help="""\
Wildcard expression of the name of the files in the dataset to use. Multiple files (separated by comma) can be related through the wildcard character.
Each instance of the match is passed to the worker function. Example: susyEvents*.root,susyTriggers*.root""",
        default="*.root", metavar="FORMAT")
    #    inputOpts.add_option("-z", "--no-suffix", action="store_true", dest="noSuffix", help="Do not append the jobId to the output file names")
    parser.add_option_group(inputOpts)

    runtimeOpts = OptionGroup(parser, "Runtime options", "These options can be changed for each job submission.")
    runtimeOpts.add_option("-b", "--bsub-options", dest="bsubOptions", help='options to pass to bsub command. -J and -cwd are set automatically. Example: -R "rusage[pool=2048]" -q 1nh', metavar="OPTIONS", default="-q 1nh")
    runtimeOpts.add_option("-d", "--debug", action="store_true", dest="debug", help="")
    runtimeOpts.add_option("-L", "--LXPLUS", dest="LXPLUS", help="LXPLUS node instance to use for job submission and monitoring", default=LXPLUS, metavar="NAME")
    runtimeOpts.add_option("-r", "--resubmit", action="store_true", dest="resubmit", help="Resubmit the job")
    runtimeOpts.add_option("-R", "--recover", action="store_true", dest="recover", help="Recover failed jobs")
    runtimeOpts.add_option("-T", "--threads-per-disk", dest="threadsPerDisk", help="Number of parallel threads per disk", type="int", default=1, metavar="NUM")
    runtimeOpts.add_option("-M", "--max-jobs", dest="maxJobs", help="Maximum (approximate) number of concurrent jobs to be submitted", type="int", default=MAXCONN, metavar="NUM")
    runtimeOpts.add_option("-j", "--jobs", dest="jobs", help="Jobs to submit", default="", metavar="JOB1[,JOB2[,JOB3...]]")
    runtimeOpts.add_option("-S", "--auto-resubmit", action="store_true", dest="autoResubmit", help="Automatically resubmit failed jobs")
    runtimeOpts.add_option("-t", "--no-submit", action="store_true", dest="noSubmit", help="Compile and quit")
    parser.add_option_group(runtimeOpts)

    (options, args) = parser.parse_args()

    sys.argv = sys.argv[0:1]

    DEBUG = options.debug
    LXPLUS = options.LXPLUS
    
    resubmit = options.resubmit or options.recover

    if not resubmit and (len(args) < 2 or not options.workspace):
        parser.print_usage()
        sys.exit(1)

    jobConfig = {}
    if resubmit:
        workspace = os.path.realpath(args[0])
        if not os.path.exists(workspace) or not os.path.exists(workspace + '/jobconfig.py'):
            raise RuntimeError("{0} is not a valid workspace".format(workspace))
        
        jobConfig["workspace"] = workspace

        if not options.recover:
            shutil.rmtree(workspace + '/logs')
            os.mkdir(workspace + '/logs')
    else:
        workspace = os.path.realpath(options.workspace)
        if os.path.exists(workspace):
            raise RuntimeError("{0} already exists".format(workspace))

        jobConfig["workspace"] = workspace

        datasets = args[0:len(args) - 1]
        macro = args[len(args) - 1]
        if ':' in macro:
            jobConfig["macro"] = os.path.realpath(macro.split(':')[0])
            jobConfig["analyzer"] = macro.split(':')[1]
        else:
            jobConfig["macro"] = os.path.realpath(macro)
            if '/' in macro:
                jobConfig["analyzer"] = macro[macro.rfind('/') + 1:macro.rfind('.')]
            else:
                jobConfig["analyzer"] = macro[0:macro.rfind('.')]

        jobConfig["includePaths"] = options.includePaths.strip()
        if options.libraries.strip():
            jobConfig["libraries"] = options.libraries.strip().split(',')
        else:
            jobConfig["libraries"] = []

        jobConfig["analyzerArguments"] = options.analyzerArguments.strip()
            
        jobConfig["outputDir"] = options.outputDir.strip()
        jobConfig["reduceCmd"] = options.reduceCmd.strip()
        jobConfig["setenv"] = options.setenv.strip().rstrip(';')

    print 'Using {0} as workspace'.format(workspace)

    # start the master server (creates the workspace)
    server = DispatchServer(jobConfig)

    os.chdir(workspace)

    # disk names
    diskNames = map(lambda p: p.replace('/data/', ''), glob.glob('/data/*'))

    # get list of files per disk
    if not resubmit:
        if DEBUG: print "preparing the lists of input files"

        if not options.nameFormat:
            raise RuntimeError("Input file name format")
        
        nameSpecs = map(str.strip, options.nameFormat.split(','))
        mainName = nameSpecs[0]
        mainLfns = []

        for dataset in datasets:
            mainLfns += glob.glob(dataset + '/' + mainName)
        
        pfnLists = dict([(diskName, []) for diskName in diskNames])
        for lfn in mainLfns:
            try:
                pfn = os.readlink(lfn)
                subst = re.search(mainName.replace('*', '(.*)'), lfn).group(1)
                for nameSpec in nameSpecs[1:]:
                    pfn += ',' + os.readlink(os.path.dirname(lfn) + '/' + nameSpec.replace('*', subst))
            except OSError:
                continue
                
            matches = re.match('/data/(disk[0-9]+)/', pfn)
            if not matches:
                print pfn + ' does not match valid pfn pattern'
                continue

            diskName = matches.group(1)
            pfnLists[diskName].append(pfn)

        filesPerJob = options.filesPerJob

	for diskName in diskNames:
            iJob = 0
            while iJob * filesPerJob < len(pfnLists[diskName]):
                jobName = diskName + '_job' + str(iJob)
                with open('inputs/' + jobName, 'w') as inputList:
                    for path in pfnLists[diskName][iJob * filesPerJob:(iJob + 1) * filesPerJob]:
                        inputList.write(path + '\n')
    
                iJob += 1

    if options.noSubmit:
        print 'no-submit flag is set. exiting.'
        sys.exit(0)

    print 'Starting request server'

    server.start()

    print 'Threading out job submission tasks'

    if options.jobs:
        specified = options.jobs.split(',')
    else:
        specified = []

    threadsPerDisk = options.threadsPerDisk if options.threadsPerDisk > 0 else 1

    maxJobsPerThread = options.maxJobs / threadsPerDisk / len(diskNames)
    if maxJobsPerThread == 0: maxJobsPerThread = 1

    for diskName in diskNames:
	if options.recover:
	    jobNames = map(lambda name: name.replace("logs/", "").replace(".fail", ""), glob.glob("logs/{0}_*.fail".format(diskName)))
	else:
	    jobNames = map(lambda name: name.replace("inputs/", ""), glob.glob('inputs/{0}_*'.format(diskName)))

	if len(specified) > 0:
            filtered = []
            for spec in specified:
                filtered += filter(lambda name: re.match(spec.replace('*', '.*'), name) is not None, jobNames)
	    jobNames = filtered

	if len(jobNames) == 0:
	    print 'No jobs to submit for {0}. Skipping.'.format(diskName)
	    continue

        jobsPerThread = len(jobNames) / threadsPerDisk
        if jobsPerThread == 0: jobsPerThread = 1
            
        for iTh in range(threadsPerDisk):
            jobList = dict.fromkeys(jobNames[iTh * jobsPerThread:(iTh + 1) * jobsPerThread], (0, CREATED))
            if len(jobList) == 0: continue
            threadName = diskName if threadsPerDisk == 1 else diskName + '-' + str(iTh)

            server.runThread(threadName, jobList, options.bsubOptions, options.autoResubmit, maxJobsPerThread)


    server.monitorJobs()

    print '\nDone.'
