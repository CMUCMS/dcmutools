#!/usr/bin/env python

from __future__ import print_function

import os
import re
import subprocess
import random

disks = []
with open('/proc/mounts') as mounts:
    for mount in mounts:
        matches = re.match('/dev/sd[b-z][0-9]+ ([^ ]+)', mount)
        if not matches: continue
        disks.append(matches.group(1))

def dscp(source, lfn, logfunc):
    if os.path.islink(lfn):
        logfunc(lfn, 'already exists as LFN')
        return False
    
    if lfn[0:7] != '/store/':
        logfunc('LFN must start with /store/')
        return False

    lfdir = os.path.dirname(lfn)

    if os.path.exists(lfdir):
        if not os.path.isdir(lfdir):
            logfunc('Cannot make', lfdir, 'into a directory')
            return False
    else:
        os.makedirs(lfdir)

    try:
        target = max(disks, key = lambda d : os.statvfs(d).f_bavail)
    except:
        target = disks[random.randint(0, len(disks) - 1)]

    pfn = lfn.replace('/store', target)
    
    pfdir = os.path.dirname(pfn)
    
    if os.path.exists(pfdir):
        if not os.path.isdir(pfdir):
            logfunc('Cannot make', pfdir, 'into a directory')
            return False
    else:
        os.makedirs(pfdir)
    
    proc = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', source, pfn])
    while proc.poll() is None: pass
    if proc.returncode != 0:
        logfunc('Copy failed')
        return False
    
    os.symlink(pfn, lfn)

    return True

if __name__ == '__main__':

    import sys
    
    try:
        source, lfn = sys.argv[1:]
    except IndexError:
        logfunc('Usage: dscp.py SOURCE LFN')
        sys.exit(1)

    try:
        if not dscp(source, lfn, print):
            raise RuntimeError('')
    except:
        print(sys.exc_info()[0:2])
        sys.exit(1)
