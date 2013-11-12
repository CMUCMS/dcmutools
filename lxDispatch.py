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
import SocketServer
import subprocess
import Queue

TIMEOUT = 30
MINPORT = 40000
MAXPORT = 40000
LXPLUS = "lxplus5"
USER = os.environ['USER']

DEBUG = False

CREATED = 0
SUBMITTED = 1
RUNNING = 2
DONE = 3
FAILED = 4
SUCCLOGGED = 5
FAILLOGGED = 6

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

class JobListManager:
    """
    Store and manipulate the list of jobs. Each job list is a dictionary that maps a job name to its (id, status).
    A pair of job list and accompanying lock is kept in self._jobLists, whose index is the job group name.
    """
    
    def __init__(self):
	self._jobLists = {}
	self._maxLen = 0

    def addList(self, name_, list_, lock_):
	self._jobLists[name_] = (list_, lock_)

    def getList(self, name_):
        try:
            return self._jobLists[name_][0]
        except KeyError:
            return {}

    def getLock(self, name_):
        try:
            return self._jobLists[name_][1]
        except KeyError:
            return None

    def getJobGroups(self):
        return self._jobLists.keys()

    def getIdNameMapping(self):
        mapping = {}
        for jobGroup, jobLock in self._jobLists.items():
            jobList = jobLock[0]
            lock = jobLock[1]

            lock.acquire()
            try:
                for jobName, idStatus in jobList.items():
                    if idStatus[0] != 0 and idStatus[1] not in (SUCCLOGGED, FAILLOGGED): mapping[idStatus[0]] = (jobGroup, jobName)
            finally:
                lock.release()

        return mapping
    
    def updateDisplay(self):
	if DEBUG: sys.stdout.write('\n')
	
        line = '\r'
    
	for name in self._jobLists.keys():
            jobList = self._jobLists[name][0]
            lock = self._jobLists[name][1]

	    nJobs = len(jobList)

            lock.acquire()
            try:
                nRunning = countJobs(jobList, RUNNING)
                nSucceeded = countJobs(jobList, (DONE, SUCCLOGGED))
                nFailed = countJobs(jobList, (FAILED, FAILLOGGED))
            finally:
                lock.release()

            nPending = nJobs - nRunning - nSucceeded - nFailed

            line += name + ': [' + str(nSucceeded + nFailed)
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
                if DEBUG: print 'communication error during handshake'
                self.request.close()
                return

            if DEBUG: print 'handling request from job {0}'.format(jobName)

            jobLists = self.server.jobLists
            for jobGroup in jobLists.getJobGroups():
                if jobName in jobLists.getList(jobGroup):
                    self.server.queues[jobGroup].put((jobName, self.request))
                    break
            else:
                if DEBUG: print 'job {0} not found in any list'.format(jobName)


    class DownloadRequestServer(SocketServer.TCPServer):
        """
        TCP server with additional data members
        """

        queues = {}

        def __init__(self, addr_, jobLists_, queues_):
            SocketServer.TCPServer.__init__(self, addr_, DispatchServer.DownloadRequestHandler)
            self.jobLists = jobLists_
            self.queues = queues_

            self.timeout = TIMEOUT

        def __del__(self):

            for queue in self.queues.values():
                while not queue.empty():
                    jobName, sock = queue.get()
                    sock.close()

        def process_request(self, request_, clientAddr_):
            """
            Overriding SocketServer.TCPServer method to avoid closing the socket
            """
            self.finish_request(request_, clientAddr_)
            

    def __init__(self, config_):
        """
        Workspace setup:
        [workspace]/inputs
                   /logs
                   /LSFOUT
                   /jobConfig.py
        If workspace already exists, config_ will be overridden by the job configuration saved in jobConfig.py.
        After the workspace has been set up, the function performs the following checks:
         - ROOT macro integrity
         - Output directory existence
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
            os.mkdir(self._workspace + '/LSFOUT')

            with open(self._workspace + '/jobconfig.py', 'w') as configFile:
                configFile.write('jobConfig = ' + str(config_) + '\n')
    
        # check source
        macro = config_["macro"]
        print 'Checking and compiling ' + macro
        if os.path.exists(macro):
            with open(macro) as macroFile:
                for line in macroFile:
                    if re.match('^[ ]*class[ ]+' + config_["analyzer"], line): break
                else:
                    raise RuntimeError("analyzer not found in macro")
        else:
            raise RuntimeError("Macro not found")
    
        # compile the source
        if DEBUG: print "setting up ROOT environment"

        import ROOT
    
        rootlogon = ROOT.gEnv.GetValue("Rint.Logon", "")
        if rootlogon:
            ROOT.gROOT.Macro(rootlogon)
    
        for lib in config_["libraries"].split(','):
            ROOT.gSystem.Load(lib)
    
        if config_["includePaths"]:
            ROOT.gSystem.AddIncludePath(config_["includePaths"])
    
        if DEBUG: print "compiling macro"
    
        ROOT.gROOT.LoadMacro(macro + '+')
    
        # check output directory
        self._outputDir = config_["outputDir"]
        if ':' in self._outputDir:
            host, dir = tuple(self._outputDir.split(':'))
            sshProc = subprocess.Popen(['ssh', '-oStrictHostKeyChecking=no', host, '[ -d {0} ]'.format(dir)])
            while sshProc.poll() is None: time.sleep(1)
    
            dirExists = (sshProc.returncode == 0)
        else:
            dirExists = os.path.isdir(self._outputDir)
    
        if not dirExists:
            raise RuntimeError("output directory not found")

        jobLists = JobListManager()
        queues = {}

        # check the reducer
        self._reduceCmd = config_["reduceCmd"]
        if self._reduceCmd:
            if DEBUG: print 'checking reduceCmd'

            class Alarm(Exception):
                pass

            def alarmHndl(num, frame):
                raise Alarm

            defHndl = signal.signal(signal.SIGALRM, alarmHndl)
            
            redProc = subprocess.Popen(self._reduceCmd + " /tmp/" + USER + "/JOBGROUP", shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            redProc.stdin.write('\n')

            signal.alarm(5)
            try:
                while redProc.poll() is None: pass
                signal.alarm(0)
            except Alarm:
                print 'ReduceCmd did not respond to EOF'
                raise

            if DEBUG: print 'reduceCmd output file name: ' + redProc.stdout.readline()

            signal.signal(signal.SIGALRM, defHndl)

        # check port availability and start server
        port = MINPORT
        while port <= MAXPORT:
            try:
                self._reqServer = DispatchServer.DownloadRequestServer(('', port), jobLists, queues)
                break
            except:
                port += 1
        else:
            raise RuntimeError("Allowed ports all in use")

        self._updateFlag = threading.Event()
        self._exitFlag = threading.Event()

        self._jobThreads = []

        if DEBUG: print 'exiting server Ctor'

    def startServer(self):
 
        thread = threading.Thread(target = self._reqServer.serve_forever, name = 'reqServer')
        thread.daemon = True
        thread.start()
        if DEBUG: print 'started request server'       

    def runThread(self, threadName_, jobList_, bsubOptions_, autoResubmit_):

        lock = threading.Lock()
        queue = Queue.Queue()
        self._reqServer.jobLists.addList(threadName_, jobList_, lock)
        self._reqServer.queues[threadName_] = queue
    
        thread = threading.Thread(target = self.lxDispatch, args = (threadName_, bsubOptions_, autoResubmit_), name = threadName_)
        thread.start()
        if DEBUG: print 'started thread ' + threadName_

        self._jobThreads.append(thread)

    def monitorJobs(self):

        self._reqServer.jobLists.updateDisplay()

        monitorCom = Queue.Queue()
        monitorThread = threading.Thread(target = self.monitorLSFStatus, args = tuple([monitorCom]), name = 'monitor')
        monitorThread.start()

        try:
            loops = 0
            while True:
                for thread in self._jobThreads:
                    thread.join(TIMEOUT)
    
                    if self._updateFlag.isSet():
                        self._reqServer.jobLists.updateDisplay()
                        self._updateFlag.clear()
    		    
                    if thread.isAlive(): break
                else:
                    monitorCom.put('lastcheck')
                    break
    
                loops += 1
                if loops % 3 == 1:
                    monitorCom.put('check')
                    
        except:
            print "\nInterrupted. exiting.."

            monitorCom.put('quit')
            self._exitFlag.set()
            for thread in self._jobThreads:
                thread.join()

            if DEBUG: print 'all job threads joined'

            monitorThread.join()
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

    def lxDispatch(self, jobGroup_, bsubOptions_, autoResubmit_):

        jobList = self._reqServer.jobLists.getList(jobGroup_)
        lock = self._reqServer.jobLists.getLock(jobGroup_)

        reducerDir = ''

        if self._reduceCmd:
            if DEBUG: print 'starting reducer process'
            remoteDir = '/tmp/{0}/{1}'.format(USER, jobGroup_)
            reducerLog = self._workspace + '/logs/reduce_' + jobGroup_ + '.log'
            redProc = subprocess.Popen("ssh -oStrictHostKeyChecking=no {0} 'echo HOSTNAME $HOSTNAME;rm -rf {1} > /dev/null 2>&1;mkdir {1};{2} {1} {3}'".format(LXPLUS, remoteDir, self._reduceCmd, reducerLog),
                shell = True, stdout = subprocess.PIPE, stdin = subprocess.PIPE, stderr = subprocess.STDOUT)

            output = redProc.stdout.readline()
            while output and 'HOSTNAME' not in output:
                output = redProc.stdout.readline()
            if not output:
                raise RuntimeError("reducer did not start")

            reducerHost = output.split()[1]

            if DEBUG: print 'Reducer of {0} runs on {1}'.format(jobGroup_, reducerHost)

            reducerDir = '{0}:{1}'.format(reducerHost, remoteDir) 

        thread = threading.Thread(target = self.bsubClient, args = (bsubOptions_, jobList, reducerDir, lock), name = 'bsub-' + jobGroup_)
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
                    self.bsubClient(bsubOptions_, failedJobs, reducerDir)
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
    
            if DEBUG: print 'received request from ', jobName
    
            try:
                sock.send('ready')
                response = sock.recv(1024)
                sock.send('OK')

                if response == 'done' or response == 'fail':
                    if response == 'done' and self._reduceCmd:
                        files = []
                        while True:
                            output = sock.recv(1024).strip()
                            sock.send('OK')
                            if not output: break
                            if ':' in output: output = output[output.find(':') + 1:]
                            files.append(output)

                        for file in files:
                            redProc.stdin.write(file + '\n')
                            if DEBUG: print 'passing {0} to reduceCmd'.format(file)
                    
                    if DEBUG: print 'setting status of {0} to {1}'.format(jobName, response)

                    status = DONE if response == 'done' else FAILED

                    lock.acquire()
                    jobList[jobName] = (jobList[jobName][0], status)
                    lock.release()
       
                    self._updateFlag.set()
            except:
                print '\n{0}: communication with {1} failed'.format(jobGroup_, jobName)
            finally:
                sock.close()

        if self._reduceCmd:
            try:
                redProc.stdin.write('\n')
                reduced = []
                while True:
                    path = redProc.stdout.readline().strip()
                    if not path: break
                    reduced.append(path)

                if DEBUG: print 'reduced files: ', reduced
                    
                while redProc.poll() is None: time.sleep(2)
    
                for path in reduced:
                    fileName = os.path.basename(path)
                    fileName = fileName[0:fileName.rfind('.')] + '_' + jobGroup_ + fileName[fileName.rfind('.'):]
                    scpCommand = 'scp -oStrictHostKeyChecking=no {0}:{1} {2}/{3}'.format(reducerHost, path, self._outputDir, fileName)
                    if DEBUG: print scpCommand
                    scpProc = subprocess.Popen(scpCommand, shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
                    while scpProc.poll() is None: time.sleep(2)
            except:
                print '\nreduce step failed in {0}'.format(jobGroup_)
            finally:
                redProc.terminate()
                while redProc.poll() is None: time.sleep(2)
                with open(reducerLog, 'a') as logFile:
                    logFile.write('Executed on node ' + reducerHost + '\n')

        if DEBUG: print 'lxDispatch {0} returning'.format(jobGroup_)

    def bsubClient(self, bsubOptions_, jobList_, reducerDir_, lock_ = None):
    
        def closeConnection(session_, force_):
            if force_: session.terminate()
            else: session_.stdin.write('exit\n')
            while session_.poll() is None: time.sleep(1)
    
        sshSession = subprocess.Popen("ssh -oStrictHostKeyChecking=no -T " + LXPLUS, shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    
        try:
            for jobName in jobList_.keys():
                for nAttempt in range(10):
                    if self._exitFlag.isSet():
                        closeConnection(sshSession, False)
                        return
        
                    if DEBUG: print 'submitting job {0}, trial {1}'.format(jobName, nAttempt)
        
                    sshSession.stdin.write("bsub {0} -J {1} -cwd {2}/LSFOUT 'cd {3};eval `scramv1 runtime -sh`;export PYTHONPATH=$PYTHONPATH:{2};lxClient.py {2} {1} {4} {5} > {2}/logs/{1}.run 2>&1'\n".format(bsubOptions_, jobName, self._workspace, os.environ["CMSSW_BASE"], self._reqServer.server_address[1], reducerDir_))
        
                    bsubout = sshSession.stdout.readline().strip()
                    while bsubout and 'submitted' not in bsubout:
                        bsubout = sshSession.stdout.readline().strip()
        
                    if not bsubout:
                        if DEBUG: print 'bsub for {0} failed. reconnecting'
                        closeConnection(sshSession, True)
                        sshSession = subprocess.Popen("ssh -oStrictHostKeyChecking=no -T " + LXPLUS, shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
                        continue
        
                    matches = re.search('<([0-9]+)>', bsubout)
                    if not matches:
                        raise RuntimeError('bsub returned ' + bsubout)
         
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
        finally:
            closeConnection(sshSession, False)
    
        if DEBUG: print 'submitted {0} jobs'.format(len(jobList_))


    def bkill(self, jobList_, lock_ = None):
    
        lock_.acquire()
        killList = filterJobs(jobList_, (SUBMITTED, RUNNING))
        lock_.release()
    
        sshSession = subprocess.Popen("ssh -oStrictHostKeyChecking=no -T " + LXPLUS, shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    
        try:
            for jobName, idStatus in killList.items():
                if DEBUG: print 'killing job {0}'.format(jobName)
        
                sshSession.stdin.write('bkill {0}\n'.format(idStatus[0]))
        
                if lock_: lock_.acquire()
                jobList_[jobName] = (jobList_[jobName][0], FAILED)
                if lock_: lock_.release()
        finally:
            try:
                sshSession.stdin.write('exit\n')
                while sshSession.poll() is None: time.sleep(2)
            except:
                pass
        
        self._updateFlag.set()


    def monitorLSFStatus(self, queue_):
        
        run = True
        while run:
            try:
                try:
                    message = queue_.get(True, 120)
                    if DEBUG: print 'monitorLSFStatus: command ' + message
                    if message == 'check':
                        pass
                    elif message == 'quit':
                        break
                    elif message == 'lastcheck':
                        run = False
                except Queue.Empty:
                    if self._exitFlag.isSet():
                        run = False
                    else:
                        continue

                # get all un-logged jobs
                idToJob = self._reqServer.jobLists.getIdNameMapping()
        
                if DEBUG: print 'jobs with valid LSF ID:', idToJob
            
                if len(idToJob):
                    if DEBUG: print "querying LSF status"
            
                    bjobsp = subprocess.Popen('ssh -oStrictHostKeyChecking=no {0} "bjobs -a"'.format(LXPLUS), shell = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
                    bjobsout = bjobsp.stdout
                    while bjobsp.poll() is None: time.sleep(2)
        
                    if DEBUG: print "bjobs returned with code " + str(bjobsp.returncode)
                    if bjobsp.returncode != 0: continue
        
                    bjobsout.readline()
        
                    while True:
                        line = bjobsout.readline()
                        if not line: break
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
                            elif statusWord == 'DONE':
                                status = SUCCLOGGED
                                log = 'done'
                            elif statusWord == 'EXIT':
                                status = FAILLOGGED
                                log = 'fail'

                            if message == 'lastcheck':
                                # no need to lock because no other thread should be alive
                                if jobList[jobName][1] == DONE:
                                    status = SUCCLOGGED
                                    log = 'done'
                                elif jobList[jobName][1] == FAILED:
                                    status = FAILLOGGED
                                    log = 'fail'

                            lock.acquire()
                            if jobList[jobName] < status:
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
    
            except:
                print '\nmonitorLSFStatus crashed', sys.exc_info()[0]
                print 'restarting..'
            
        if DEBUG: print 'monitorLSFStatus returning'
            

if __name__ == '__main__':

    try:
        if not os.environ['CMSSW_BASE']:
            raise EnvironmentError
    except:
        print "CMSSW_BASE not set"

    from optparse import OptionParser, OptionGroup

    parser = OptionParser(usage="usage: %prog [options] dataset [dataset2 [dataset3 ...]] macro")

    execOpts = OptionGroup(parser, "Job execution options", "These options will be saved in the job configuration and used will be used at each job execution.")
    execOpts.add_option("-w", "--workspace", dest='workspace', help='Name of the job directory', metavar="DIR")
    execOpts.add_option("-I", "--include", dest="includePaths", help="Include path for compilation (CMSSW workspace is automatically added)", default="", metavar="-IDIR1 [-IDIR2 [-IDIR3 ...]]")
    execOpts.add_option("-l", "--lib", dest="libraries", help="Libraries to load", default="libSusyEvent.so", metavar="LIB1[,LIB2[,LIB3...]]")
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
    runtimeOpts.add_option("-T", "--threads-per-disk", dest="threadsPerDisk", help="Number of parallel threads per disk", default=1, metavar="NUM")
    runtimeOpts.add_option("-j", "--jobs", dest="jobs", help="Jobs to submit", default="", metavar="JOB1[,JOB2[,JOB3...]]")
    runtimeOpts.add_option("-a", "--auto-resubmit", action="store_true", dest="autoResubmit", help="Automatically resubmit failed jobs")
    runtimeOpts.add_option("-t", "--no-submit", action="store_true", dest="noSubmit", help="Compile and quit")
    parser.add_option_group(runtimeOpts)
    
    (options, args) = parser.parse_args()

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
        jobConfig["libraries"] = options.libraries.strip()
        jobConfig["outputDir"] = options.outputDir.strip()
        jobConfig["reduceCmd"] = options.reduceCmd.strip()

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

    server.startServer()

    print 'Threading out job submission tasks'

    if options.jobs:
        specified = options.jobs.split(',')
    else:
        specified = []

    threadsPerDisk = options.threadsPerDisk if options.threadsPerDisk > 0 else 1

    for diskName in diskNames:
	if options.recover:
	    jobNames = map(lambda name: name.replace("logs/", "").replace(".fail", ""), glob.glob("logs/{0}_*.fail".format(diskName)))
	else:
	    jobNames = map(lambda name: name.replace("inputs/", ""), glob.glob('inputs/{0}_*'.format(diskName)))

	if len(specified) > 0:
	    jobNames = filter(lambda name: name in specified, jobNames)

	if len(jobNames) == 0:
	    print 'No jobs to submit for {0}. Skipping.'.format(diskName)
	    continue

        jobsPerThread = len(jobNames) / threadsPerDisk
            
        for iTh in range(threadsPerDisk):
            jobList = dict.fromkeys(jobNames[iTh * jobsPerThread:(iTh + 1) * jobsPerThread], (0, CREATED))
            threadName = diskName if threadsPerDisk == 1 else diskName + '-' + str(iTh)

            server.runThread(threadName, jobList, options.bsubOptions, options.autoResubmit)

    try:
        server.monitorJobs()
    except:
        if DEBUG: print 'Exception: ', sys.exc_info()[0]

    print '\nDone.'
