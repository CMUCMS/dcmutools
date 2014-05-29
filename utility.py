import sys
import re
import traceback

# LIST OF DISKS

disks = []
mountMap = {}
mounts = open('/proc/mounts')
for mount in mounts:
    matches = re.match('(/dev/sd[b-z][0-9]+) ([^ ]+)', mount)
    if not matches: continue
    disks.append(matches.group(2))
    mountMap[matches.group(2)] = matches.group(1)

mounts.close()

# STACK TRACE AS STRING

def excDump():
    return ''.join(traceback.format_exception(*sys.exc_info()))
