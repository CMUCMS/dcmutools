#!/usr/bin/env python

import os
import sys
import traceback

from utility import disks

def searchDir(disk, path):
    result = []
    
    pfpath = disk + '/' + path
    lfpath = '/store/' + path
    try:
        for entry in os.listdir(pfpath):
            if os.path.isdir(pfpath + '/' + entry):
                result += searchDir(disk, path + '/' + entry)
            elif not os.path.exists(lfpath + '/' + entry):
                result.append(pfpath + '/' + entry)
            
        if not os.path.exists(lfpath):
            result.append(pfpath)
    except:
        print pfpath
        traceback.print_exc()

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
