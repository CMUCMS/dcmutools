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
import Queue

TMPDIR = os.environ['TMPDIR']

DEBUG = False

SCP = ['scp', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet']

logLock = threading.Lock()

def log(*args):
    logLock.acquire()
    try:
        message = ': '
        for arg in args:
            message += str(arg) + ' '

        sys.stdout.write(time.strftime('%H:%M:%S') + message + '\n')
        sys.stdout.flush()
        if sys.stdout.fileno() != 1: os.fsync(sys.stdout) # stdout is redirected to a file
    except:
        pass
    finally:
        logLock.release()

class ServerConnection(object):

    key = ''
    jobName = ''
    host = ''
    port = 0
    
    def __init__(self, service_):
        nAttempt = 0
        while nAttempt < 10:
            response = ''
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if DEBUG: log('Connect to', (ServerConnection.host, ServerConnection.port))
                self.sock.connect((ServerConnection.host, ServerConnection.port))
                if DEBUG: log(ServerConnection.key)
                self.sock.send(ServerConnection.key)
                response = self.sock.recv(1024)
                if DEBUG: log(ServerConnection.host + ' says ' + response)
                if response != 'JOB':
                    raise Exception()
                if DEBUG: log(ServerConnection.jobName)
                self.sock.send(ServerConnection.jobName)
                response = self.sock.recv(1024)
                if DEBUG: log(ServerConnection.host + ' says ' + response)
                if response != 'SVC':
                    raise Exception()
                if DEBUG: log(service_)
                self.sock.send(service_)
                response = self.sock.recv(1024)
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
    
    
def downloadFiles(workspace_, jobName_, key_, host_, port_, queue_):

    inputList = []
    with open(workspace_ + '/inputs/' + jobName_) as inputListFile:
        for line in inputListFile:
            inputList.append(line)

    while len(inputList):
        inputLine = inputList[0]
        diskName = re.match('/data/(disk[0-9]+)/', inputLine).group(1)
        conn = ServerConnection(diskName)
        if conn.sock is None:
            queue_.put('FAILED')
            break

        try:
            remoteFiles = map(str.strip, inputLine.split(','))
            localPaths = []
            for remotePath in remoteFiles:
                log('downloading file', remotePath)
    
                if DEBUG: log('DLD')
                conn.sock.send('DLD')
                response = conn.sock.recv(1024)
                if DEBUG: log(ServerConnection.host + ' says ' + response)

                if response != 'GO':
                    raise RuntimeError('no permission')

                localPath = TMPDIR + '/' + key_ + '/input/' + jobName_ + '/' + remotePath[remotePath.rfind('/') + 1:]
    
                scpProc = subprocess.Popen(SCP + [host_ + ':' + remotePath, localPath])
                while scpProc.poll() is None: time.sleep(2)
                if scpProc.returncode != 0:
                    raise RuntimeError('copy failure')
    
                localPaths.append(localPath)
            else:
                # copied all files in this line
                if DEBUG: log('DONE')
                
                conn.sock.send('DONE')
                        
                queue_.put(tuple(localPaths))
                
                log('successfully downloaded', localPaths)

                inputList.pop(0)

        except:
            log('download request failed in', jobName_, 'due to', sys.exc_info()[0:2], '. retrying')
            continue

    else:
        # successfully downloaded all files
        queue_.put('DONE')

    log('downloader returning')


if __name__ == '__main__':
    
    workspace = sys.argv[1]
    jobName = sys.argv[2]

    logFile = open(workspace + '/logs/' + jobName + '.log', 'w', 0)
    sys.stdout = logFile
    sys.stderr = logFile

    sys.argv = ['', '-b']

    import ROOT

    sys.path.append(workspace)

    log('loading config')

    from jobconfig import jobConfig
    from macro import arguments # worker source code loaded to ROOT here

    ServerConnection.key = jobConfig['key']
    ServerConnection.jobName = jobName
    ServerConnection.host = jobConfig['serverHost']
    ServerConnection.port = jobConfig['serverPort']
    
    # report to dispatch

    conn = ServerConnection('dispatch')
    if DEBUG: log('RUNNING')
    conn.sock.send('RUNNING')
    conn = None # delete the object to close the connection
        
    outputDir = TMPDIR + '/' + jobConfig['key'] + '/output/' + jobName
    inputDir = TMPDIR + '/' + jobConfig['key'] + '/input/' + jobName

    shutil.rmtree(outputDir, ignore_errors = True)
    shutil.rmtree(inputDir, ignore_errors = True)
    os.makedirs(outputDir)
    os.makedirs(inputDir)
    
    # instantiate the analyzer class and initialize
    # set _threaded to True in order to have concurrent download and run
    analyzer = getattr(ROOT, jobConfig['analyzer'])()
    analyzer.run._threaded = True

    log('initialization')
    if not analyzer.initialize(outputDir, *arguments):
        raise RuntimeError("Worker class initialization")
    
    # thread off the downloader function
    
    fileNameQueue = Queue.Queue()
    
    log('starting downloader')
    
    thread = threading.Thread(target = downloadFiles, args = (workspace, jobName, jobConfig['key'], jobConfig['serverHost'], jobConfig['serverPort'], fileNameQueue))
    thread.daemon = True
    thread.start()

    try:
        # start looping over downloaded files
        
        while True:
            if DEBUG: log('waiting for file names')

            downloaded = []
            lastPass = False
            block = True
            while True:
                try:
                    fileNames = fileNameQueue.get(block = block)
                    block = False # only block for the first time

                    if fileNames == 'FAILED':
                        raise RuntimeError('Download failed')
                    elif fileNames == 'DONE':
                        lastPass = True
                    else:
                        analyzer.addInput(*fileNames)
                        downloaded.append(fileNames)
                except Queue.Empty:
                    break
            
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
    
        if not analyzer.finalize():
            raise RuntimeError("analyzer.finalize()")
    
        # copy output files to remote host

        outputContents = os.listdir(outputDir)
        
        if jobConfig['reducer'] != 'None' and len(outputContents) == 1:
            log('Copying output to', jobConfig['serverHost'])
            
            localPath = outputDir + '/' + outputContents[0]
    
            remoteFileName = jobConfig['outputFile']
            remoteFileName = remoteFileName[0:remoteFileName.rfind('.')] + '_' + jobName + remoteFileName[remoteFileName.rfind('.'):]
    
#            conn = ServerConnection('reduce')
#            if DEBUG: log('DEST')
#            conn.sock.send('DEST')
#            response = conn.sock.recv(1024)
#            if DEBUG: log(ServerConnection.host + ' says ' + response)
#            remotePath = response + '/' + remoteFileName
# Choosing to run reducer "offline" and not as a service; adds stability with a price of little time rag after the jobs are done. darun jobs will upload the output to $TMPDIR/{key}
            remotePath = jobConfig['serverTmpDir'] + '/' + jobConfig['key'] + '/reduce/input/' + remoteFileName

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
                    copyCommands.append(SCP + [localPath, jobConfig['outputNode'] + ':' + remotePath])

        for command in copyCommands:
            iTry = 0
            while iTry < 3:
                copyProc = subprocess.Popen(command, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
                while copyProc.poll() is None: time.sleep(2)
        
                if copyProc.returncode == 0:
                    if DEBUG: log(command)

#                    conn.sock.send(remoteFileName)
#                    response = conn.sock.recv(1024)
#                    if DEBUG: log(ServerConnection.host + ' says ' + response)
#                    conn = None
#                    if response == 'FAILED':
#                        continue
                        
                    break
                else:
                    if DEBUG: log(command, 'failed')

#                    conn.sock.send('FAIL')
#                    conn.sock.recv(1024)
#                    conn = None

                iTry += 1
            else:
                raise RuntimeError('copy failure')
    
        # report to dispatcher
    
        conn = ServerConnection('dispatch')
        if DEBUG: log('DONE')
        conn.sock.send('DONE')
        conn = None

        log('Done.')

    except:
        log('Exception while running')
        traceback.print_exc()

        analyzer.clearInput()

        conn = ServerConnection('dispatch')
        if DEBUG: log('FAILED')
        if conn.sock:
            conn.sock.send('FAILED')
        conn = None
        
        log('Aborted.')
