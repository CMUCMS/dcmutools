#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import re
import subprocess
import random
import traceback
import shutil

import dsrm
from utility import *

def dscp(source, lfn, logfunc = print, force = False):
    logfunc('dscp', source, lfn)
    
    if os.path.islink(lfn):
        if force:
            try:
                dsrm.rmlink(lfn)
            except:
                logfunc('Failed to remove', lfn)
                return False
        else:
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

    target = random.choice(disks)

    pfn = lfn.replace('/store', target)
    
    pfdir = os.path.dirname(pfn)
    
    if os.path.exists(pfdir):
        if not os.path.isdir(pfdir):
            logfunc('Cannot make', pfdir, 'into a directory')
            return False
    else:
        os.makedirs(pfdir)

    if ':' in source:
        proc = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', source, pfn], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        while proc.poll() is None: pass
        if proc.returncode != 0:
            response = ''
            while True:
                line = subprocess.stdout.readline()
                if not line: break
                response += line

            logfunc('Copy failed:', response)
            return False
    else:
        try:
            shutil.copyfile(source, pfn)
        except:
            logfunc('Copy failed:\n', excDump())
            return False
    
    os.symlink(pfn, lfn)

    return True

if __name__ == '__main__':

    try:
        source, lfn = sys.argv[1:]
    except:
        print('Usage: dscp.py SOURCE LFN')
        sys.exit(1)

    try:
        if not dscp(source, lfn, logfunc = print):
            raise RuntimeError('')
    except:
        traceback.print_exc()
        sys.exit(1)
