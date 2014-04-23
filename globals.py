import os

MINPORT = 40000
MAXPORT = 40001
DCMUHOST = "dcmu00"
TERMNODE = 'lxplus'
HTMLDIR = '/afs/cern.ch/user/y/yiiyama/www/dap/tasks'
USER = os.environ['USER']
#try:
#    DEFAULTENV = 'cd ' + os.environ['CMSSW_BASE'] + ';scram runtime -sh'
#except KeyError:
#    DEFAULTENV = ''
DEFAULTENV = 'cd /afs/cern.ch/user/y/yiiyama/cmssw/SLC6Ntuplizer5314;scram runtime -sh'
try:
    TMPDIR = os.environ['TMPDIR']
except KeyError:
    TMPDIR = '/tmp/' + USER

DEBUG = False
SERVERONLY = False # for debugging purpose
