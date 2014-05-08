#!/usr/bin/env python

import sys
import os
import time
import re
import random
import subprocess
import threading

DEBUG = False

srcdir = os.path.dirname(os.path.realpath(__file__))
if srcdir not in sys.path:
    sys.path.append(srcdir)
from disks import disks

def dsDownload(source, dest, deleteFile = False, deleteDir = False, singleThread = False):
    if not dest.startswith('/store'):
        raise RuntimeError('Invalid destination path')

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
        line = lsProc.stdout.readline()
        if DEBUG: print line
        if not line: break
        files.append((line.split()[4], int(line.split()[1])))

    while lsProc.poll() is None: time.sleep(1)

    if DEBUG: print 'xrd returned with code', lsProc.returncode
    
    if lsProc.returncode != 0:
        raise RuntimeError('xrd Error: ' + err)

    if len(files) == 0:
        # source can be a single file
        statProc = subprocess.Popen(['xrd', 'eoscms', 'existfile', source], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        while True:
            line = statProc.stdout.readline()
            if not line: break
            if 'The file exists.' in line:
                disk = random.choice(disks)
                cont = [source]
                execDownload(cont, dest.replace('/store', disk), None, deleteFile)
                if len(cont) == 1:
                    pfn = cont[0]
                    if DEBUG: print 'ln -s', pfn, dest + '/' + os.path.basename(pfn)
                    os.symlink(pfn, dest + '/' + os.path.basename(pfn))
                    return 0
                else:
                    return 1

        return 0

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
        printLock = threading.Lock()
        for disk in disks:
            if DEBUG: print 'Start thread', disk
            thread = threading.Thread(target = execDownload, args = (lists[disk], dest.replace('/store', disk), printLock, deleteFile), name = disk)
            thread.start()
            threads[disk] = thread
    
        success = True
        for disk in disks:
            threads[disk].join()
            if DEBUG: print 'Thread', threads[disk].getName(), 'returned'
            
            for pfn in lists[disk]: # now points to local pfn
                if DEBUG: print 'ln -s', pfn, dest + '/' + os.path.basename(pfn)
                os.symlink(pfn, dest + '/' + os.path.basename(pfn))
    
            if len(lists) != listSizes[disk]: success = False

    else:
        success = True
        disksToRun = []
        for disk in disks: disksToRun.append(disk)
        while len(disksToRun):
            disk = disksToRun.pop(0)

            cont = [lists[disk].pop()]
            execDownload(cont, dest.replace('/store', disk), None, deleteFile)
            if len(cont) == 1:
                pfn = cont[0]
                if DEBUG: print 'ln -s', pfn, dest + '/' + os.path.basename(pfn)
                os.symlink(pfn, dest + '/' + os.path.basename(pfn))
            else:
                success = False
                pass

            if len(lists[disk]) != 0:
                disksToRun.append(disk)

    if deleteDir and success:
        print 'Deleting EOS directory', source
        delProc = subprocess.Popen(['xrd', 'eoscms', 'rmdir', source])
        while delProc.poll() is None: time.sleep(1)

    return 0
    

def execDownload(files, pfDir, lock, deleteFile = False):
    pfns = []
    while True:
        try:
            file = files.pop(0)
        except IndexError:
            break
        
        pfn = pfDir + '/' + os.path.basename(file)

        if lock: lock.acquire()
        print file, '->', pfn
        if lock: lock.release()

        cpProc = subprocess.Popen(['xrdcp', '-f', 'root://eoscms//eos/cms' + file, pfn], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        while cpProc.poll() is None: time.sleep(1)
        out, err = cpProc.communicate()

        if cpProc.returncode == 0:
            if deleteFile:
                delProc = subprocess.Popen(['xrd', 'eoscms', 'rm', file])
                while delProc.poll() is None: time.sleep(1)
                if delProc.returncode != 0:
                    if lock: lock.acquire()
                    print '****', file, 'not deleted'
                    if lock: lock.release()

            pfns.append(pfn)
        else:
            if lock: lock.acquire()
            print '****Failed to copy', file, ':', err
            if lock: lock.release()

    files += pfns

if __name__ == '__main__':

    from optparse import OptionParser

    parser = OptionParser(usage = 'dsDownload.py [options] source dest')
    parser.add_option('-d', '--delete-file', action = 'store_true', dest = 'deleteFile', help = 'Delete successfully copied files.')
    parser.add_option('-D', '--delete-dir', action = 'store_true', dest = 'deleteDir', help = 'Delete source directory at the end.')
    parser.add_option('-L', '--linear', action = 'store_true', dest = 'linear', help = 'Single thread')

    options, args = parser.parse_args()

    source, dest = args

    returncode = dsDownload(source, dest, deleteFile = options.deleteFile, deleteDir = options.deleteDir, singleThread = options.linear)

    if returncode is not None:
        sys.exit(returncode)
    else:
        sys.exit(1)
