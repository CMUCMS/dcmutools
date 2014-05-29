#!/usr/bin/env python

### This is a wrapper script to run ROOT applications over a list of files downloaded from a storage server.
### The ROOT source code must contain a definition of class (whose name is given in the job configuration).
### The class must have at least the following four interfaces:
###  bool initialize(const char* output_directory, ...)
###  void addInput(const char* input_path[, const char* input_path2, ...])
###  bool run()
###  void clearInput()
###  bool finalize()
### The reason for run not taking the list of inputs directly is due to some oddity in the threaded running
### of PyRoot functions. (Segfault observed when trying to pass a string argument to a function with
### _threaded = True)
### clearInput() must be callable multiple times consecutively and should not crash even after abnormal
### termination of run().
### A parallel thread to download the files runs along the main loop over the downloaded files.

import sys
import os
import re
import traceback
import socket
import subprocess
import time
import shutil
import threading
import ftplib
import Queue

TMPDIR = os.environ['TMPDIR']

DEBUG = False

SCP = ['scp', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet']

logLock = threading.Lock()

def log(*args):
    with logLock:
        try:
            message = ': '
            for arg in args:
                message += str(arg) + ' '
    
            sys.stdout.write(time.strftime('%H:%M:%S') + message + '\n')
            sys.stdout.flush()
            if sys.stdout.fileno() != 1: os.fsync(sys.stdout) # stdout is redirected to a file
        except:
            pass

class ServerConnection(object):

    key = ''
    jobName = ''
    host = ''
    port = 0

    class ConnectionRejected(Exception):
        pass
    
    def __init__(self, service_):
        nAttempt = 0
        while nAttempt < 10:
            response = ''
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if DEBUG: log('Connect to', (ServerConnection.host, ServerConnection.port))
                self.sock.connect((ServerConnection.host, ServerConnection.port))

                if DEBUG: log(ServerConnection.jobName + ' ' + ServerConnection.key)
                response = self.communicate(ServerConnection.jobName + ' ' + ServerConnection.key)
                if DEBUG: log(ServerConnection.host + ' says ' + response)
                if response != 'SVC':
                    raise Exception()
                
                if DEBUG: log(service_)
                response = self.communicate(service_)
                if DEBUG: log(ServerConnection.host + ' says ' + response)
                if response == 'ACCEPT':
                    break
                elif response == 'WAIT':
                    self.sock.close()
                    time.sleep(5)
                    continue
                else:
                    raise Exception()
            except:
                if DEBUG: log('Socket connection failed', sys.exc_info()[0:2])
                
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                    self.sock.close()
                except:
                    pass

                self.sock = None
                
                if response == 'REJECT':
                    log('Connection rejected by server. Job quitting.')
                    break
                    
                nAttempt += 1
        else:
            log('Number of connection attempt exceeded limit.')
            
    def __del__(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except:
            pass

    def communicate(self, message, listen = True):
        self.sock.send(message)
        if listen:
            response = self.sock.recv(1024)
            if response == 'REJECT':
                raise ServerConnection.ConnectionRejected
            
            return response
    
    
def downloadFiles(workspace_, jobName_, taskID_, host_, queue_):

    inputList = []
    with open(workspace_ + '/inputs/' + jobName_) as inputListFile:
        for inputRow in inputListFile:
            inputList.append(eval(inputRow)) # [(dev, path), ...]; one line corresponds to one set of input
            
    while len(inputList):
        inputLine = inputList[0]
        if DEBUG: log('input line:' + str(inputLine))

        localPaths = []
        for diskName, remotePath in inputLine:
            log('downloading file', remotePath)

            localPath = TMPDIR + '/' + taskID_ + '/input/' + jobName_ + '/' + remotePath[remotePath.rfind('/') + 1:]

            conn = ServerConnection(diskName)
            if conn.sock is None:
                queue_.put('FAILED')
                break
    
            try:
                if DEBUG: log('DLD')
                response = conn.communicate('DLD')
                if DEBUG: log(ServerConnection.host + ' says ' + response)

                if response != 'OK':
                    raise RuntimeError('no permission')

                try:
                    # first try ftp
                    ftp = ftplib.FTP('dcmu00', 'anonymous')
                    response = ftp.cwd(os.path.dirname(remotePath))
                    if int(response.split()[0]) != 250:
                        raise Exception
                    with open(localPath, 'wb') as remoteFile:
                        response = ftp.retrbinary('RETR ' + os.path.basename(remotePath), remoteFile.write)
                        if int(response.split()[0]) != 226:
                            raise Exception
                except:
                    # try scp
                    scpProc = subprocess.Popen(SCP + [host_ + ':' + remotePath, localPath], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
                    while scpProc.poll() is None: time.sleep(2)
                    out = scpProc.communicate()[0]
                    if scpProc.returncode != 0:
                        raise RuntimeError('copy failure: ' + out)
                finally:
                    try:
                        ftp.quit()
                    except:
                        pass

                if DEBUG: log('DONE')
                conn.communicate('DONE', listen = False)
    
                localPaths.append(localPath)

            except:
                log('download request failed in', jobName_, 'due to', sys.exc_info()[0:2], '. retrying')
                break
            finally:
                conn = None
        else:
            # copied all files in this line
            log('successfully downloaded', localPaths)
           
            queue_.put(tuple(localPaths))
            inputList.pop(0)
            
    else:
        # successfully downloaded all files
        queue_.put('DONE')

    log('downloader returning')


def heartbeat():
    while True:
        conn = ServerConnection('dispatch')
        if conn.sock:
            conn.communicate('HB')
            conn = None

        time.sleep(60)

        
if __name__ == '__main__':

    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option('-p', '--print', dest = 'printToStdout', action = 'store_true', help = 'Do not redirect stdout to log file')

    options, args = parser.parse_args()
    
    workspace, jobName, key = args

    sys.argv = ['', '-b']

    import ROOT
    ROOT.gErrorIgnoreLevel = ROOT.kError

    sys.path.append(workspace)
    from jobconfig import jobConfig

    if not options.printToStdout:
        logFile = open(jobConfig['logDir'] + '/' + jobName + '.log', 'a', 0)
        sys.stdout = logFile
        sys.stderr = logFile

    log('darun.py', workspace, jobName, key)

    from macro import arguments # worker source code loaded to ROOT here

    ServerConnection.key = key
    ServerConnection.jobName = jobName
    ServerConnection.host = jobConfig['serverHost']
    ServerConnection.port = jobConfig['serverPort']
    
    # report to dispatch

    conn = ServerConnection('dispatch')
    if DEBUG: log('RUNNING')
    conn.communicate('RUNNING')
    conn = None # delete the object to close the connection
        
    outputDir = TMPDIR + '/' + jobConfig['taskID'] + '/output/' + jobName
    inputDir = TMPDIR + '/' + jobConfig['taskID'] + '/input/' + jobName

    shutil.rmtree(outputDir, ignore_errors = True)
    shutil.rmtree(inputDir, ignore_errors = True)
    os.makedirs(outputDir)
    os.makedirs(inputDir)
    
    # instantiate the analyzer class and initialize
    # set _threaded to True in order to have concurrent download and run
    analyzer = getattr(ROOT, jobConfig['analyzer'])()
    analyzer.run._threaded = True

    log('initialize:', outputDir, *arguments)
    if not analyzer.initialize(outputDir, *arguments):
        raise RuntimeError("Worker class initialization")
    
    # thread off the downloader function
    
    fileNameQueue = Queue.Queue()
    
    log('starting downloader')
    
    downloadThread = threading.Thread(target = downloadFiles, args = (workspace, jobName, jobConfig['taskID'], jobConfig['serverHost'], fileNameQueue))
    downloadThread.daemon = True
    downloadThread.start()

    log('starting heartbeat')

    heartbeatThread = threading.Thread(target = heartbeat)
    heartbeatThread.daemon = True
    heartbeatThread.start()

    try:
        # start looping over downloaded files
        
        while True:
            if DEBUG: log('waiting for file names')

            downloaded = []
            lastPass = False
            block = True
            while True:
                try:
                    fileNames = fileNameQueue.get(block = block, timeout = 180)
                    block = False # only block for the first time

                    if fileNames == 'FAILED':
                        raise RuntimeError('Download failed')
                    elif fileNames == 'DONE':
                        lastPass = True
                        break
                    else:
                        analyzer.addInput(*fileNames)
                        downloaded.append(fileNames)
                except Queue.Empty:
                    if downloadThread.isAlive():
                        if block: # Empty raised because of timeout
                            continue
                        else: # Just checked if there are additional files
                            break
                    else: # Downloader crashed
                        raise RuntimeError('Downloader not responding')
            
            log('received file names', downloaded)

            if len(downloaded) == 0:
                break
        
            log('run')
            if not analyzer.run():
                raise RuntimeError("analyzer.run()")
            log('processed', downloaded)

            analyzer.clearInput()
    
            for fileNames in downloaded:
                for fileName in fileNames:
                    os.remove(fileName)

            if lastPass:
                break

        log('finalize')
        if not analyzer.finalize():
            raise RuntimeError("analyzer.finalize()")

        analyzer = None
    
        # copy output files to remote host

        outputContents = sorted(os.listdir(outputDir))
        log('Produced file(s)', outputContents)

        if len(outputContents) == 0:
            raise RuntimeError("No output");

        useReducer = jobConfig['reducer'] != 'None' and len(outputContents) == 1 # if only one type of output is produced
        outputIsLFN = jobConfig['outputNode'] == jobConfig['serverHost'] and jobConfig['outputDir'][0:7] == '/store/'

        remoteCommands = []
        
        if useReducer:
            log('Copying output for reducer in', jobConfig['serverHost'])
            
            localPath = outputDir + '/' + outputContents[0]
    
            remoteFileName = jobConfig['outputFile']
            remoteFileName = remoteFileName[0:remoteFileName.rfind('.')] + '_' + jobName + remoteFileName[remoteFileName.rfind('.'):]
  
#            conn = ServerConnection('reduce')
#            if DEBUG: log('DEST')
#            response = conn.communicate('DEST')
#            if DEBUG: log(ServerConnection.host + ' says ' + response)
#            remotePath = response + '/' + remoteFileName
# Choosing to run reducer "offline" and not as a service; adds stability with a price of little time rag after the jobs are done. darun jobs will upload the output to $TMPDIR/{taskID}
            remotePath = jobConfig['serverWorkDir'] + '/reduce/input/' + remoteFileName

            copyCommands = [SCP + [localPath, jobConfig['serverHost'] + ':' + remotePath]]
            
        else:
            log('Copying output to', jobConfig['outputNode'])

            copyCommands = []

            for localFileName in outputContents:
                localPath = outputDir + '/' + localFileName
    
                remoteFileName = localFileName
                remoteFileName = remoteFileName[0:remoteFileName.rfind('.')] + '_' + jobName + remoteFileName[remoteFileName.rfind('.'):]

                remotePath = jobConfig['outputDir'] + '/' + remoteFileName
               
                if jobConfig['outputNode'] == 'eos':
                    copyCommands.append(['cmsStage', localPath, remotePath])
                else:
                    if outputIsLFN:
                        remotePath = jobConfig['serverWorkDir'] + '/dscp/' + remoteFileName
                        
                    copyCommands.append(SCP + [localPath, jobConfig['outputNode'] + ':' + remotePath])

        if outputIsLFN:
            dscp = ServerConnection('dscp')
            if dscp is None:
                raise RuntimeError('Cannot establish connection to DSCP server')
        else:
            dscp = None

        for command in copyCommands:
            iTry = 0
            while iTry < 3:
                log(command)
                copyProc = subprocess.Popen(command)
                while copyProc.poll() is None: time.sleep(2)

                if copyProc.returncode == 0:
                    if DEBUG: log('Copy success')

                    #if useReducer:                    
                    #    conn.sock.send(remoteFileName)
                    #    response = conn.sock.recv(1024)
                    #    if DEBUG: log(ServerConnection.host + ' says ' + response)
                    #    conn = None
                    #    if response == 'FAILED':
                    #        continue

                    if dscp:
                        remotePath = command[-1]
                        if ':' in remotePath: # has to be the case..
                            remotePath = remotePath[remotePath.find(':') + 1:]

                        log('dscp', remotePath)
                        
                        if DEBUG: log(remotePath)
                        response = dscp.communicate(remotePath)
                        if DEBUG: log(ServerConnection.host + ' says ' + response)
                        if response != 'OK':
                            raise RuntimeError('DSCP failed')
                        
                    break
                else:
                    log(command, 'failed')

#                    conn.communicate('FAIL')
#                    conn = None

                iTry += 1
            else:
                if dscp:
                    dscp.communicate('FAIL')
                    dscp = None
                    
                raise RuntimeError('copy failure')

        if dscp:
            dscp.communicate('DONE', listen = False)
            dscp = None        

        # report to dispatcher
        
        conn = ServerConnection('dispatch')
        if DEBUG: log('DONE')
        conn.communicate('DONE')
        conn = None

        log('Done.')

    except:
        log('Exception while running')
        traceback.print_exc()

        analyzer.clearInput()

        conn = ServerConnection('dispatch')
        if DEBUG: log('FAILED')
        if conn.sock:
            conn.communicate('FAILED')
        conn = None
        
        log('Aborted.')


    if not options.printToStdout:
        logFile.close()
