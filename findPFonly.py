#!/usr/bin/env python

import os
import sys

srcdir = os.path.dirname(os.path.realpath(__file__))
if srcdir not in sys.path:
    sys.path.append(srcdir)
from disks import disks

def searchDir(disk, path):
    result = []
    
    pfpath = disk + '/' + path
    lfpath = '/store/' + path
    try:
        for entry in os.listdir(pfpath):
            if os.path.isdir(pfpath + '/' + entry):
                result += searchDir(disk, path + '/' + entry)
            
        if not os.path.exists(lfpath):
            result.append(pfpath)
    except:
        print pfpath, sys.exc_info()[0:2]

    return result


if __name__ == '__main__':
    try:
        path = sys.argv[1]
    except IndexError:
        path = ''

    if path.startswith('/store'):
        path = path.replace('/store/', '')
    
    list = []
    for disk in disks:
        list += searchDir(disk, path)

    for pfn in list:
        print pfn
