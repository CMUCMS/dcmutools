#!/usr/bin/env python

import sys
import os
import time
import re
import random
import traceback
import subprocess
import signal
import threading

from utility import disks

DEBUG = False

def dsDownload(source, dest, deleteFile = False, deleteDir = False, singleThread = False):
    if not dest.startswith('/store'):
	print 'Invalid destination path'
	return 1

    dest = os.path.realpath(dest)

    print 'Checking destination', dest

    dirs = [dest]
    while not os.path.exists(dirs[0]): dirs.insert(0, os.path.dirname(dirs[0]))
    for iD in range(1, len(dirs)):
        dir = dirs[iD]
        os.mkdir(dir)
        os.chmod(dir, 0775)
        for disk in disks:
            pfDir = dir.replace('/store', disk)
            os.mkdir(pfDir)
            os.chmod(pfDir, 0775)

    print 'Obtaining list of files to download'

    if DEBUG: print 'xrd eoscms dirlist', source

    lsProc = subprocess.Popen(['xrd', 'eoscms', 'dirlist', source], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    
    files = []
    while True:
        line = lsProc.stdout.readline().strip()
        if DEBUG: print line
        if not line: break
        files.append((line.split()[4], int(line.split()[1])))

    while lsProc.poll() is None: time.sleep(1)

    if DEBUG: print 'xrd returned with code', lsProc.returncode
    
    if lsProc.returncode != 0:
	print 'xrd Error:', err
	return 1

    if len(files) == 0:
        # source can be a single file
        statProc = subprocess.Popen(['xrd', 'eoscms', 'existfile', source], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        exists = False
        while True:
            line = statProc.stdout.readline().strip()
            if not line: break
            if 'The file exists.' in line:
                exists = True
                break

        returncode = 0
        
        sigIntHndl = signal.signal(signal.SIGINT, signal.SIG_IGN)

        if exists:
            disk = random.choice(disks)
            cont = [source]
            
            execDownload(cont, dest.replace('/store', disk), deleteFile)
    
            if len(cont) == 1:
                pfn = cont[0]
                if DEBUG: print 'ln -s', pfn, dest + '/' + os.path.basename(pfn)
                try:
                    os.symlink(pfn, dest + '/' + os.path.basename(pfn))
                except OSError:
                    print pfn
                    traceback.print_exc()
            else:
                returncode = 1
        else:
            returncode = 2

        signal.signal(signal.SIGINT, sigIntHndl)                    

        return returncode

    files = sorted(files, cmp = lambda x, y: x[1] - y[1], reverse = True)

    print 'Downloading', len(files), 'files'

    firstdisk = random.choice(disks)

    if DEBUG: print 'First disk', firstdisk

    lists = {}
    for disk in disks:
        lists[disk] = []

    dir = 1
    disk = firstdisk
    for file in files:
        lists[disk].append(file[0])
        disk = disks[(disks.index(disk) + dir) % len(disks)]
        if disk == firstdisk:
            dir *= -1

    listSizes = dict([(disk, len(lists[disk])) for disk in disks])

    if DEBUG:
        print 'Files to download:'
        for disk in disks:
            print ' ', disk, ':', listSizes[disk]

    if not singleThread:
        threads = {}
        exitFlag = threading.Event()
        printLock = threading.Lock()
        for disk in disks:
            if DEBUG: print 'Start thread', disk
            thread = threading.Thread(target = execDownload, args = (lists[disk], dest.replace('/store', disk), deleteFile, exitFlag, printLock), name = disk)
            thread.start()
            threads[disk] = thread

        try:
            for disk in disks:
                threads[disk].join()
                if DEBUG: print 'Thread', threads[disk].getName(), 'returned'

        except KeyboardInterrupt:
            exitFlag.set()
            for disk in disks:
                threads[disk].join()
                if DEBUG: print 'Thread', threads[disk].getName(), 'returned'

        sigIntHndl = signal.signal(signal.SIGINT, signal.SIG_IGN)
        
        success = True
        for disk in disks:
            for pfn in lists[disk]: # now points to local pfn
                if DEBUG: print 'ln -s', pfn, dest + '/' + os.path.basename(pfn)
                try:
                    os.symlink(pfn, dest + '/' + os.path.basename(pfn))
                except OSError:
                    print pfn
                    raise
        
            if len(lists) != listSizes[disk]: success = False

        signal.signal(signal.SIGINT, sigIntHndl)

    else:
        allFiles = []
        exhausted = False
        while not exhausted:
            exhausted = True
            for disk in disks:
                try:
                    allFiles.append(lists[disk].pop(0))
                except IndexError:
                    continue

                exhausted = False

        nTotal = len(allFiles)

        exitFlag = threading.Event()

        sigIntHndl = signal.signal(signal.SIGINT, lambda signum, frame: exitFlag.set())

        execDownload(allFiles, dest.replace('/store', disk), deleteFile, exitFlag)

        for pfn in allFiles: # now points to local pfn
            if DEBUG: print 'ln -s', pfn, dest + '/' + os.path.basename(pfn)
            try:
                os.symlink(pfn, dest + '/' + os.path.basename(pfn))
            except OSError:
                print pfn
                raise
            
        signal.signal(signal.SIGINT, sigIntHndl)

        success = len(allFiles) == nTotal

    if deleteDir and success:
        print 'Deleting EOS directory', source
        delProc = subprocess.Popen(['xrd', 'eoscms', 'rmdir', source])
        while delProc.poll() is None: time.sleep(1)

    return 0
    

def execDownload(files, pfDir, deleteFile = False, exitFlag = None, printLock = None):
    remotePaths = []
    while len(files):
        remotePaths.append(files.pop(0))

    for file in remotePaths:
        pfn = pfDir + '/' + os.path.basename(file)

        if printLock: printLock.acquire()
        print file, '->', pfn
        if printLock: printLock.release()

        cpProc = subprocess.Popen(['xrdcp', '-f', 'root://eoscms//eos/cms' + file, pfn], stdout = subprocess.PIPE, stderr = subprocess.PIPE, preexec_fn = lambda: signal.signal(signal.SIGINT, signal.SIG_IGN))
        while cpProc.poll() is None: time.sleep(1)
        out, err = cpProc.communicate()

        if cpProc.returncode == 0:
            if deleteFile:
                delProc = subprocess.Popen(['xrd', 'eoscms', 'rm', file])
                while delProc.poll() is None: time.sleep(1)
                if delProc.returncode != 0:
                    if printLock: printLock.acquire()
                    print '****', file, 'not deleted'
                    if printLock: printLock.release()

            files.append(pfn)
        else:
            if printLock: printLock.acquire()
            print '****Failed to copy', file, ':', err
            if printLock: printLock.release()

        if exitFlag and exitFlag.set(): break


if __name__ == '__main__':

    from optparse import OptionParser

    parser = OptionParser(usage = 'dsDownload.py [options] source dest')
    parser.add_option('-d', '--delete-file', action = 'store_true', dest = 'deleteFile', help = 'Delete successfully copied files.')
    parser.add_option('-D', '--delete-dir', action = 'store_true', dest = 'deleteDir', help = 'Delete source directory at the end.')
    parser.add_option('-L', '--linear', action = 'store_true', dest = 'linear', help = 'Single thread')

    options, args = parser.parse_args()

    source, dest = args

    try:
	returncode = dsDownload(source, dest, deleteFile = options.deleteFile, deleteDir = options.deleteDir, singleThread = options.linear)
    except:
        traceback.print_exc()
	returncode = 1

    sys.exit(returncode)
