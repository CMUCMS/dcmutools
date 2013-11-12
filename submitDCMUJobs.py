#!/usr/bin/env python

from __future__ import print_function

import sys
import os
import signal
import subprocess
import httplib
import urllib
import re
import time
import string
import stat
from optparse import OptionParser, OptionGroup

ndisk = 7
nodeURL = 'dcmu00.cern.ch'

parser = OptionParser(usage="usage: %prog [options] dataset [dataset2 [dataset3 ...]] macro",
    description='''DCMU node job scheduler. Pass the logical path (/store/..) to the datasets and the root macro to be run.
Macro must be compilable with the "+" option in ROOT, and has to define a function of the form
void myMacro(..., TObjArray* urls, TObjArray* outputNames)
where myMacro is the name of the file (myMacro.cc), ... is the additional arguments specified with the -a option, urls is the array of TObjStrings of the file paths, and outputNames is the array of the names of the output files specified with the -s and -o options.
Jobs will be scheduled based on the list of files on each disk unless an external list of scheduling strings is passed, in which case the macro function has to take the form
void myMacro(..., TString const& dataset, TObjArray* tasks, TObjArray* outputNames)''')

runtimeOpts = OptionGroup(parser, "Runtime Options", "These options can be changed for each job submission.")
runtimeOpts.add_option("-d", "--jobs-per-disk", type="int", dest='jobsPerDisk', help='Number of concurrent jobs per disk', metavar="NUM", default=3)
runtimeOpts.add_option("-m", "--maximum-jobs", type="int", dest='maxJobs', help='Maximum number of jobs to submit', metavar="NUM", default=-1)
parser.add_option_group(runtimeOpts)

crtTimeOpts = OptionGroup(parser, "Creation-time Options", "These options will be saved in the job configuration at the job creation.")
crtTimeOpts.add_option("-n", "--files-per-job", type="int", dest='filesPerJob', help='Number of files per job', metavar="NUM", default=1)
crtTimeOpts.add_option("-J", "--job-name", dest='jobName', help='Name of the job directory', metavar="NAME")
crtTimeOpts.add_option("-x", "--external-list", dest='externalList', help="External list of strings to be used for job scheduling")
crtTimeOpts.add_option("-a", "--macro-arguments", dest='macroArguments', help='Additional arguments to the macro', metavar="ARGS", default='')
crtTimeOpts.add_option("-f", "--file-name", dest="nameFormat", help="Wildcard expression of the name of the files in the dataset to use.", default="*.root", metavar="FORMAT")
crtTimeOpts.add_option("-s", "--output-dir", dest="outputDir", help="Output directory", default="", metavar="DIR")
crtTimeOpts.add_option("-o", "--output-files", dest="outputFiles", help="Output file names", default="", metavar="[FILE1.root,FILE2.root,...]")
crtTimeOpts.add_option("-z", "--no-suffix", action="store_true", dest="noSuffix", help="Do not append the jobId to the output file names")
crtTimeOpts.add_option("-I", "--include", dest="includePaths", help="Include path for compilation (CMSSW workspace is automatically added)", default="", metavar="DIR1[,DIR2,...]")
crtTimeOpts.add_option("-l", "--lib", dest="libraries", help="Libraries to load", default="libSusyEvent.so", metavar="LIB1[,LIB2,...]")
parser.add_option_group(crtTimeOpts)

parser.add_option("-r", "--resubmit", dest="resubDir", help="Resubmit the job in DIR", default="", metavar="DIR")
parser.add_option("-R", "--recover", action="store_true", dest="recover", help="Recover failed jobs")
parser.add_option("-t", "--no-submit", action="store_true", dest="noSubmit", help="Compile and quit")

(options, args) = parser.parse_args()

# check environment

outputIsLFN = False
addSuffix = True

resubmit = False
if options.resubDir:
    resubmit = True

fileLists = list()
for idisk in range(0, ndisk):
    fileLists.append(list())

outputFiles = []
externalList = ''

if resubmit:
    workspace = os.getcwd() + '/' + options.resubDir
    while workspace[len(workspace) - 1] == '/':
        workspace = workspace[0:len(workspace) - 1]
        
    os.chdir(workspace)
    config = file('exec.cfg', 'r')
    for line in config:
        matches = re.match('datasets[ ]=[ ]((?:[^,]+,?)+)', line.strip())
        if matches:
            datasets = matches.group(1).split(',')
        
        matches = re.match('outputDir[ ]=[ ](.+)', line.strip())
        if matches:            
            outputDir = matches.group(1)
            if outputDir[0:7] == '/store/':
                outputIsLFN = True

        matches = re.match('outputFiles[ ]=[ ]((?:[^,]+,?)+)', line.strip())
        if matches:
            outputFiles = matches.group(1).split(',')

        matches = re.match('noSuffix[ ]=[ ](.+)', line.strip())
        if matches and matches.group(1) == 'True':
            addSuffix = False

        matches = re.match('externalList[ ]=[ ](.+)', line.strip())
        if matches:
            externalList = matches.group(1)

        matches = re.match('filesPerJob[ ]=[ ]([1-9][0-9]*)', line.strip())
        if matches:
            filesPerJob = int(matches.group(1))

        matches = re.match('macro[ ]=[ ](.+)', line.strip())
        if matches:
            macro = matches.group(1)

    if externalList == '':
        for idisk in range(0, ndisk):
            urllist = file('disk' + str(idisk), 'r')
            for url in urllist:
                fileLists[idisk].append(url)
    else:
        listFile = file(externalList, 'r')
        iLine = 0
        for scId in listFile:
            if outputIsLFN:
                fileLists[iLine % ndisk].append(scId)
                iLine += 1
            else:
                fileLists[0].append(scId)

    if options.recover:
        recoverList = []
        wsFiles = os.listdir(workspace)
        for idisk in range(0, ndisk):
            ifile = 0
            while ifile < len(fileLists[idisk]):
                jobId = 'disk' + str(idisk) + '_' + str(ifile) + '_' + str(ifile + filesPerJob)
                completed = False
                for name in wsFiles:
                    if jobId in name and '.done' in name:
                        completed = True
                        break

                if not completed:
                    recoverList.append(jobId)

                ifile += filesPerJob

        if len(recoverList) == 0:
            print("No jobs to recover")
            sys.exit(0)

        print("Recover jobs:")
        print(recoverList)

    for name in os.listdir(workspace):
        if '.log' in name or '.fail' in name or '.term' in name or (not options.recover and '.done' in name):
            os.unlink(workspace + '/' + name)
            
else:
    # check the validity of the arguments

    datasets = args[0:len(args) - 1]
    macro = os.getcwd() + '/' + args[len(args) - 1]

    outputDir = options.outputDir
    if outputDir[0:7] == '/store/':
        outputIsLFN = True
    elif outputDir == '':
        outputDir = '~/scratch0/' + options.jobName

    while outputDir[len(outputDir) - 1] == '/':
        outputDir = outputDir[0:len(outputDir) - 1]

    matches = re.match('((?:[^,]+,?)+)', options.outputFiles)
    if matches:
        outputFiles = matches.group(1).split(',')

    addSuffix = not options.noSuffix

    nameFormat = re.escape(options.nameFormat)
    nameFormat = nameFormat.replace('\\?', '.').replace('\\*', '.*')
    
    try:
        lfnPat = re.compile('/store/.+/' + nameFormat)
        pfnPat = re.compile('/data/disk([0-9]+)/.+/' + nameFormat)
    except:
        raise RuntimeError('Incorrect file name format')
    
    whichp = subprocess.Popen(['which', 'root'], stdout=subprocess.PIPE)
    while whichp.poll() is None:
        pass
        
    if whichp.returncode != 0:
        raise RuntimeError('root.exe not in $PATH')
    
    pathpos = -1
    line = whichp.stdout.readline()
    while line:
        pathpos = line.find('/')
        if pathpos != -1:
            break
    
        line = whichp.stdout.readline()
        
    if pathpos == -1:
        raise RuntimeError('root.exe not in $PATH')
    
    rootpath = line[pathpos:].strip()
    
    if options.externalList:
        if len(datasets) > 1:
            raise RuntimeError('Option -x (--external-list) cannot be used with multiple datasets')
        
        try:
            os.stat(options.externalList)
        except:
            raise IOError(options.externalList + ' not found')

        externalList = os.path.basename(options.externalList)
        options.externalList = os.path.abspath(options.externalList)

    try:
        os.stat(macro)
    except:
        raise IOError(macro + ' not found')
    
    macroFile = file(macro, 'r')
    function = macro[macro.rfind('/') + 1:macro.rfind('.')]
    foundFunction = False
    for line in macroFile:
        if externalList == '':
            if re.match('[ ]*' + function + '\([^\)]*TObjArray[^,]*,[^,]*TObjArray[^,]*\)', line):
                foundFunction = True
        else:
            if re.match('[ ]*' + function + '\([^\)]*TString[^,]*,[^,]*TObjArray[^,]*,[^,]*TObjArray[^,]*\)', line):
                foundFunction = True
    
    if not foundFunction:
        raise IOError(macro + ' does not contain an executable function')

    # create the workspace and cd into it
    
    workspace = os.getcwd() + '/'
    if options.jobName:
        workspace += options.jobName
    else:
        workspace += str(int(time.time()))
        time.sleep(1)
    
    os.mkdir(workspace)
    
    print("Created " + workspace + " as working directory")
    
    os.chdir(workspace)

    if externalList == '':
        # get the file list
        
        print("Obtaining list of files...", end = ' ')
        
        con = httplib.HTTPConnection(nodeURL)
        
        for dataset in datasets:
            params = urllib.urlencode({'lfpath': dataset, 'text': 1})
            headers = {'Content-type': 'application/x-www-form-urlencoded', 'Accept': 'text/plain'}
            con.request('POST', '/get_list.php', params, headers)
            res = con.getresponse()
            allURLs = res.read().strip().split('\n')
          
            for url in allURLs:
                lfn = url.replace('http://' + nodeURL + '/', '')
                if not lfnPat.match(lfn):
                    continue
                
                params = urllib.urlencode({'lfn': lfn})
                con.request('POST', '/get_pfn.php', params, headers)
                res = con.getresponse()
                pfn = res.read().strip()
                matches = pfnPat.match(pfn)
                if not matches:
                    raise IOError('LFN ' + lfn + ' does not match the pattern ' + nameFormat)
        
                fileLists[int(matches.group(1))].append(lfn)
            
        con.close()

        nFiles = 0

        for idisk in range(0, ndisk):
            nFiles += len(fileLists[idisk])
            fileList = file('disk' + str(idisk), 'w')
            for url in fileLists[idisk]:
                fileList.write(url + '\n')

            fileList.close()

        if nFiles == 0:
            raise RuntimeError('Found 0 files in the dataset.')
        
        print("Done.")
    else:
        # copy the task list
        
        orig = file(options.externalList, 'r')
        copy = file(externalList, 'w')
        iLine = 0
        for scId in orig:
            if outputIsLFN:
                fileLists[iLine % ndisk].append(scId)
                iLine += 1
            else:
                fileLists[0].append(scId)

            copy.write(scId)

        orig.close()
        copy.close()

    # set files per job
    filesPerJob = options.filesPerJob

    # save environmental variables
    env = file('root.env', 'w')
    envContent = '''export PATH=''' + os.environ['PATH'] + '''
export LD_LIBRARY_PATH=''' + os.environ['LD_LIBRARY_PATH'] + '''
export ROOTSYS=''' + os.environ['ROOTSYS']
    if 'CMSSW_BASE' in os.environ:
        envContent += '''
export CMSSW_BASE=''' + os.environ['CMSSW_BASE']

    env.write(envContent)
    env.close()

    # execution script
    script = file('exec.cc', 'w')
    scriptContent = '''#include <stdexcept>
int exec('''
    if externalList == '' or outputIsLFN:
        scriptContent += 'TString const& disk, '

    scriptContent += '''int begin, int end){
  if(TString(gSystem->Getenv("CMSSW_BASE")) != ""){
     gSystem->AddIncludePath("-I" + TString(gSystem->Getenv("CMSSW_BASE")) + "/src");
  }
'''
    includePaths = options.includePaths.split(',')
    for path in includePaths:
        scriptContent += '  gSystem->AddIncludePath("-I' + path + '");\n'
    
    libraries = options.libraries.split(',')
    for lib in libraries:
        scriptContent += '  gSystem->Load("' + lib + '");\n'

    if addSuffix:
        scriptContent += '\n  TString jobId('
        if externalList == '' or outputIsLFN:
            scriptContent += 'disk + "_" + '
        scriptContent += 'TString::Format("%d", begin) + "_" + TString::Format("%d", end));\n'

    scriptContent += '  TString outputDir("'
    if outputIsLFN:
        scriptContent += '/data/" + disk + "/' + outputDir[7:]
    else:
        scriptContent += outputDir
    scriptContent += '");'

    scriptContent += '''
  TObjArray outputFiles;
  outputFiles.SetOwner(true);
'''
    if len(outputFiles) == 0:
        scriptContent += '  outputFiles.Add(new TObjString(outputDir'
        if addSuffix:
            scriptContent += ' + "/" + jobId'
        scriptContent += '));\n'
    else:
        for outputName in outputFiles:
            scriptContent += '  outputFiles.Add(new TObjString(outputDir + "/' + outputName[0:outputName.rfind('.')]
            if addSuffix:
                scriptContent += '_" + jobId + "'
            scriptContent += outputName[outputName.rfind('.'):] + '"));\n'
    
    scriptContent += '''
  TObjArray urls;
  urls.SetOwner(kTRUE);
  ifstream list('''

    if externalList:
        scriptContent += '"' + externalList + '"'
    else:
        scriptContent += 'disk'

    scriptContent += ''');
  int iline = 0;
  TString url;
  while(true){
     list >> url;
     if(!list.good() || url == "") break;
     if(iline >= begin){
       urls.Add(new TObjString(url));
     }
     ++iline;
     if(iline == end) break;
  }

  gROOT->LoadMacro("''' + macro + '''+");

  ''' + function + '('
    if options.macroArguments:
        scriptContent += options.macroArguments + ', '

    if externalList:
        scriptContent += '"' + datasets[0] + '", '
    
    scriptContent += '''&urls, &outputFiles);

  return 1;
}
'''
    script.write(scriptContent)
    script.close()

    # compile script
    compile = file('compile.cc', 'w')
    compileContent = '''{
  if(TString(gSystem->Getenv("CMSSW_BASE")) != ""){
     cout << "Using CMSSW_BASE " << gSystem->Getenv("CMSSW_BASE") << endl;
     gSystem->AddIncludePath("-I" + TString(gSystem->Getenv("CMSSW_BASE")) + "/src");
  }
'''
    for lib in libraries:
        compileContent += '  gSystem->Load("' + lib + '");\n'

    compileContent += '''
  gROOT->LoadMacro("''' + macro + '''+");
}'''

    compile.write(compileContent)
    compile.close()

    # save the configurations
    config = file('exec.cfg', 'w')
    config.write('datasets = ' + string.join(datasets, ',') + '\n')
    config.write('file = ' + options.nameFormat + '\n')
    config.write('macro = ' + macro + '\n')
    if options.macroArguments:
        config.write('arguments = ' + options.macroArguments + '\n')
    config.write('lib = ' + options.libraries + '\n')
    config.write('outputDir = ' + outputDir + '\n')
    config.write('outputFiles = ' + options.outputFiles + '\n')
    if externalList:
        config.write('externalList = ' + externalList + '\n')
    if not addSuffix:
        config.write('noSuffix = True\n')
    config.write('filesPerJob = ' + str(filesPerJob) + '\n')
    config.close()
#if not resubmit

# compile
print("Compiling macro...", end = ' ')
compilep = subprocess.Popen('cd ' + workspace + '; source root.env; root -b -q -l compile.cc > compile.log 2>&1', shell=True)
while compilep.poll() is None:
    pass

compileLog = file('compile.log', 'r')
for line in compileLog:
    if 'failed' in line or 'Failed' in line:
        raise RuntimeError('Failed compilation. See ' + workspace + '/compile.log')

print("Done.")

if options.noSubmit:
    print("No-submit flag is set. No job will be submitted.")
    sys.exit(0)

# set the macro to read-only until all jobs finish
os.chmod(macro, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

print("Submitting jobs...")

if outputIsLFN:
    dsname = outputDir[7:]
    if os.path.exists('/store/' + dsname):
        raise RuntimeError('Dataset /store/' + dsname + ' already exists')

    os.makedirs('/store/' + dsname)
    for idisk in range(0, ndisk):
        os.makedirs('/data/disk' + str(idisk) + '/' + dsname)

runningProcesses = list()
iLine = list()
pfDir = list()
for idisk in range(0, ndisk):
    runningProcesses.append(list())
    iLine.append(0)
    pfDir.append(outputDir)
    if outputIsLFN:
        pfDir[idisk] = '/data/disk' + str(idisk) + '/' + outputDir[7:]

linesPerJob = filesPerJob
if linesPerJob <= 0:
    linesPerJob = 0
    for idisk in range(0, ndisk):
        if len(fileLists[idisk]) > linesPerJob:
            linesPerJob = len(fileLists[idisk])

bsubPat = re.compile('<([0-9]+)>')

running = True
initialSubmissions = True
nSubmitted = 0
while running:
    try:
        nRunning = 0
        for idisk in range(0, ndisk):
            nRunning += len(runningProcesses[idisk])
            
            if len(fileLists[idisk]) == 0:
                continue
            
            if iLine[idisk] >= len(fileLists[idisk]) or len(runningProcesses[idisk]) >= options.jobsPerDisk or nSubmitted == options.maxJobs:
                initialSubmissions = False
                continue
    
            if externalList == '' or outputIsLFN:
                jobId = 'disk' + str(idisk) + '_' + str(iLine[idisk]) + '_' + str(iLine[idisk] + linesPerJob)
                execCmd = 'cd ' + workspace + '; source root.env; root -b -q -l "exec.cc(\\"disk' + str(idisk) + '\\",' + str(iLine[idisk]) + ',' + str(iLine[idisk] + linesPerJob) + ')"'
            else:
                jobId = str(iLine[idisk]) + '_' + str(iLine[idisk] + linesPerJob)
                execCmd = 'cd ' + workspace + '; source root.env; root -b -q -l "exec.cc(' + str(iLine[idisk]) + ',' + str(iLine[idisk] + linesPerJob) + ')"'
    
            if options.recover and jobId not in recoverList:
                iLine[idisk] += linesPerJob
                continue
    
            print('\r' + execCmd)

            if len(outputFiles) == 0 and addSuffix:
                os.mkdir(pfDir[idisk] + '/' + jobId)            
    
            logfile = file(workspace + '/' + jobId + '.log', 'w')
            proc = subprocess.Popen(execCmd, stdout=logfile, stderr=logfile, shell=True, preexec_fn=os.setsid)
            
            runningProcesses[idisk].append((proc, logfile))
    
            iLine[idisk] += linesPerJob
            nSubmitted += 1
    
        if initialSubmissions:
            continue

        print('                                      ', end = '\r')
        print(' Current number of jobs: ' + str(nRunning), end = '')
    
        time.sleep(5)

        for idisk in range(0, ndisk):
            for proc,logfile in runningProcesses[idisk]:
                if proc.poll() is not None:
                    runningProcesses[idisk].remove((proc, logfile))
                    logfile.close()

                    logName = logfile.name
                    jobId = logName[logName.rfind('/') + 1:len(logName) - 4]

                    if proc.returncode == 1:
                        os.rename(logName, logName.replace('.log', '.done'))
                    else:
                        os.rename(logName, logName.replace('.log', '.fail'))

                    if len(outputFiles) == 0 and addSuffix:
                        tmpDir = pfDir[idisk] + '/' + jobId
                        for fileName in os.listdir(tmpDir):
                            trueName = fileName[0:fileName.rfind('.')] + '_' + jobId + fileName[fileName.rfind('.'):]
                            pfn = pfDir[idisk] + '/' + trueName
                            os.rename(tmpDir + '/' + fileName, pfn)
                            if outputIsLFN:
                                os.symlink(pfn, outputDir + '/' + trueName)
    
                        os.rmdir(tmpDir)
                    elif outputIsLFN:
                        for fileName in outputFiles:
                            trueName = fileName[0:fileName.rfind('.')]
                            if addSuffix:
                                trueName += '_' + jobId

                            trueName += fileName[fileName.rfind('.'):]
                            os.symlink(pfDir[idisk] + '/' + trueName, outputDir + '/' + trueName)
    
        running = False
        for idisk in range(0, ndisk):
            if len(runningProcesses[idisk]) > 0 or (iLine[idisk] < len(fileLists[idisk]) and nSubmitted != options.maxJobs):
                running = True
                break

    except KeyboardInterrupt:
        for idisk in range(0, ndisk):
            for proc,logfile in runningProcesses[idisk]:
                os.killpg(proc.pid, signal.SIGTERM)
                
                logfile.close()
                logName = logfile.name
                os.rename(logName, logName.replace('.log', '.term'))

        os.chmod(macro, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        raise

os.chmod(macro, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)

errorMsg = ""
for name in os.listdir(workspace):
    if '.fail' in name:
        if not errorMsg:
            errorMsg = " There is a failed job."
        else:
            errorMsg = " There are failed jobs."

print('\nDone.' + errorMsg)
