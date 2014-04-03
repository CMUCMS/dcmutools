#!/usr/bin/env python

import sys
import os
import socket
import shutil
import subprocess
import time
import threading
import Queue
import ROOT

SERVERHOST = 'dcmu00'
TMPDIR = os.environ['TMPDIR']
TIMEOUT = 600

DEBUG = False

def openConnection(jobName_, port_):

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    nAttempt = 0
    while nAttempt < 10:
        try:
            sock.connect((SERVERHOST, port_))
            sock.send(jobName_)
            break
        except:
            if DEBUG: print 'socket connection failed ', sys.exc_info()[0]
            nAttempt += 1
    else:
        print 'cannot connect to server. exit'
        sock.close()
        return None

    return sock


def downloadFiles(workspace_, jobName_, port_, queue_, exitFlag_):

    inputList = file(workspace_ + '/inputs/' + jobName_)
    lines = []
    for line in inputList:
        lines.append(line)
    inputList.close()

    iLine = 0
    while iLine < len(lines):
        if exitFlag_.isSet():
            queue_.put('term')
            break
            
        sock = openConnection(jobName_, port_)
        if not sock: continue

        # sock closed in finally block
        try:
            response = sock.recv(1024)

            if response == 'notfound':
                queue_.put('term')
                break
            elif response != 'ready':
                continue
    
            remoteFiles = map(str.strip, lines[iLine].split(','))
            localPaths = []
            for remotePath in remoteFiles:
                if DEBUG: print 'downloading file ' + remotePath
    
                localPath = TMPDIR + '/input/' + remotePath[remotePath.rfind('/') + 1:]
    
                scpProc = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', SERVERHOST + ':' + remotePath, localPath])
                while scpProc.poll() is None: time.sleep(2)
                if scpProc.returncode != 0: break
    
                localPaths.append(localPath)
            else:
                # copied all files in this line
                sock.send('copied')
                sock.recv(1024)
                        
                queue_.put(','.join(localPaths))
                
                if DEBUG: print 'successfully downloaded', localPaths
                iLine += 1

        except:
            print 'download request failed in {0} due to {1}. retrying'.format(jobName_, sys.exc_info()[0])
            continue
        finally:
            sock.close()
    else:
        # successfully downloaded all files
        queue_.put('')

    if DEBUG: print '{0} downloader returning'.format(jobName_)
    

def cleanup(jobName_, remote_ = '', remotePaths_ = []):

    if DEBUG: print 'cleanup for ' + jobName_

    status = True
    
    inputDir = TMPDIR + '/input'
    outputDir = TMPDIR + '/output'

    try:
        shutil.rmtree(inputDir, ignore_errors = True)
    except:
        print sys.exc_info()
        status = False

    if os.path.exists(outputDir) and remote_:
        outputContents = os.listdir(outputDir)
        for file in outputContents:
            fullPath = outputDir + '/' + file
            if not os.path.isfile(fullPath): continue

            if ':' in remote_:
                (host, path) = tuple(remote_.split(':'))
            else:
                host = ''
                path = remote_

            remoteFullPath = path + '/' + file[0:file.rfind('.')] + '_' + jobName_ + file[file.rfind('.'):]

            if host == 'eos':
                copyProc = subprocess.Popen(['cmsStage', fullPath, remoteFullPath], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            else:
		if host: remoteFullPath = host + ':' + remoteFullPath
                copyProc = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', fullPath, remoteFullPath], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

            while copyProc.poll() is None: time.sleep(2)

            if copyProc.returncode != 0:
                print 'copy of ' + file + ' failed'
                status = False
                break                

            remotePaths_.append(remoteFullPath)

    try:
        shutil.rmtree(outputDir, ignore_errors = True)
    except:
        print sys.exc_info()
        status = False

    if DEBUG: print 'cleanup for {0} complete'.format(jobName_)

    return status


if __name__ == '__main__':
    
    workspace = sys.argv[1]
    jobName = sys.argv[2]
    port = int(sys.argv[3])
    remoteOutputDir = sys.argv[4]

    sys.argv = []

    sys.path.append(workspace)

    if DEBUG: print 'loading config'

    import jobconfig
    import macro
    
    # read configuration
    
    if DEBUG: print 'analyzer class instantiation'

    # instantiate the analyzer class
    analyzer = getattr(ROOT, jobconfig.jobConfig['analyzer'])()
    
    # clean input and output directories

    if DEBUG: print 'initial cleanup'
    
    if not cleanup(jobName):
        raise RuntimeError("Initial cleanup failed")
    
    inputDir = TMPDIR + '/input'
    outputDir = TMPDIR + '/output'
    
    os.mkdir(outputDir)
    os.mkdir(inputDir)
    
    # initialize

    if DEBUG: print 'initialization'

    try:
        if not analyzer.initialize(outputDir, *macro.arguments):
            raise RuntimeError("Worker class initialization")
    except:
        print sys.exc_info()
        sys.exit(1)
    
    # thread off the downloader function
    
    fileNameQueue = Queue.Queue()
    exitFlag = threading.Event()
    
    if DEBUG: print 'starting downloader for ' + jobName
    
    thread = threading.Thread(target = downloadFiles, args = (workspace, jobName, port, fileNameQueue, exitFlag))
    thread.daemon = True
    thread.start()
    
    abort = ''
    while True:
        if DEBUG: print '{0}: waiting for file names'.format(jobName)

        fileNames = fileNameQueue.get().strip()
        
        if DEBUG: print '{0}: received file names {1}'.format(jobName, fileNames)
        if not fileNames:
            break
        if fileNames == 'term':
            abort += '(download failure)'
            exitFlag.set()
            break
    
        try:
            if DEBUG: print 'run ' + fileNames
            if not analyzer.run(fileNames):
                raise RuntimeError("Worker")
        except:
            print sys.exc_info()
            abort += '(run exception)'
            exitFlag.set()
            break

        for fileName in map(str.strip, fileNames.split(',')):
            os.remove(fileName)

    try:
        if not analyzer.finalize():
            raise RuntimeError("Worker class finalization")
    except:
        print sys.exc_info()
        abort += '(finalize exception)'
        exitFlag.set()

    remotePaths = []
    if not abort and not cleanup(jobName, remoteOutputDir, remotePaths):
        abort += '(final cleanup)'
        exitFlag.set()

    if DEBUG: print 'remote paths: ', remotePaths

    while True:
       sock = openConnection(jobName, port)
       if not sock: continue
       try:
           response = sock.recv(1024)
           if response != 'ready':
               abort += '(server failure)'
               break
           if abort:
               sock.send('fail')
               sock.recv(1024)
           else:
               sock.send('done')
               sock.recv(1024)
               if jobconfig.jobConfig["reduceCmd"]:
                   for path in remotePaths:
                       sock.send(path)
                       sock.recv(1024)
                   sock.send('\n')
                   sock.recv(1024)

           break
       except:
           print 'communication error at the end of job. retrying.', sys.exc_info()
       finally:
           sock.close()
    
    if abort:
        raise RuntimeError('Job killed: ' + abort)

    print 'Done.'
