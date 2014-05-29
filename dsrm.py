#!/usr/bin/env python

import os
import sys

from utility import disks

def rmlink(lfn):
    if not os.path.islink(lfn):
        print lfn, 'is not a valid LFN'
        return False

    pfn = os.readlink(lfn)

    result = True

    try:
        os.remove(lfn)
    except OSError:
        result = False

    try:
        os.remove(pfn)
    except OSError:
        result = False

    return result

def rmlfdir(dir):
    for entry in os.listdir(dir):
        path = dir + '/' + entry
        if os.path.isdir(path):
            if not rmlfdir(path):
                return False
        else:
            if not rmlink(path):
                return False

    os.rmdir(dir)
    for disk in disks:
        try:
            os.rmdir(dir.replace('/store', disk))
        except OSError:
            print 'Could not remove', dir.replace('/store', disk)

    return True

if __name__ == '__main__':

    from optparse import OptionParser

    parser = OptionParser()

    parser.add_option('-r', '--recursive', action = 'store_true', dest = 'recursive')

    options, args = parser.parse_args()

    path = args[0]

    if not path.startswith('/store'):
        print 'LFN must start with /store'
        sys.exit(1)

    if os.path.isdir(path):
        if not options.recursive:
            print path, 'is a directory but recursive flag is not set.'
            sys.exit(1)
        else:
            print 'Recursive flag is set. Are you sure you want to remove', path, '? [Y/n]'
            while True:
                response = sys.stdin.readline().strip()
                if response == 'Y':
                    success = rmlfdir(path)
                    break
                elif response == 'n':
                    success = True
                    break

    else:
        success = rmlink(path)

    if success:
        sys.exit(0)
    else:
        sys.exit(1)
        
