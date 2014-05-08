import re

disks = []
mounts = open('/proc/mounts')
for mount in mounts:
    matches = re.match('/dev/sd[b-z][0-9]+ ([^ ]+)', mount)
    if not matches: continue
    disks.append(matches.group(1))

mounts.close()
