#!/usr/bin/env python

import os
import sys
import json
import re

from DIRAC import gLogger
from DIRAC import S_OK, S_ERROR, exit

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Get and merge output files of the task

Usage:
    %(script)s [options] TaskID

Examples:
    # Download all output files in task 165 to the current directory
    %(script)s 165

    # Download all output files in task 165 and merge all the files into larger ones, where the merged file size will not exceed 1GB
    %(script)s -g 1G 165

    # Download all dst files in task 237 to directory "/some/dir"
    %(script)s -f '*.dst' -D /some/dir 237

    # List all root file names in task 46. Do NOT download
    %(script)s -l -f '*.root' 46

    # Download all root files in task 329 to directory "output" and merge to files smaller than 800MB, reserve the root files after merge
    %(script)s -g 800M -k -f '*.root' -D output 329
""" % {'script': Script.scriptName} )

Script.registerSwitch("m:", "method=",  "Downloading method: local_rsync, dfc, cp, daemon_rsync")
Script.registerSwitch("l",  "list",     "List all output files. Do NOT download")
Script.registerSwitch("D:", "dir=",     "Output directory")
Script.registerSwitch("f:", "filter=",  "Name pattern for filtering output file")
Script.registerSwitch("r",  "reload",   "Redownload every file each time")
Script.registerSwitch("u",  "checksum", "Use checksum for file validation. Could be slow for some special situations")
Script.registerSwitch("g:", "merge=",   "Set max size for merged destination file (e.g., 500000, 2G, 700M)")
Script.registerSwitch("k",  "keep",     "Keep downloaded output files after merge. Use with -g option")

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from BESDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

from BESDIRAC.Badger.private.output.GetOutputHandler   import GetOutputHandler


downloadCounter = {}


def sizeConvert(sizeString):
  unitDict = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4, 'P': 1024**5}

  size = -1
  try:
    unit = sizeString[-1]
    if unit in unitDict:
      size = int(sizeString[:-1]) * unitDict[unit]
    else:
      size = int(sizeString)
  except:
    pass

  return size


def getTaskOutput(taskID):
  lfnList = []

  result = taskClient.getTaskJobs(taskID)
  if not result['OK']:
    print result['Message']
    return lfnList
  jobIDs = result['Value']

  outFields = ['Info']
  result = taskClient.getJobs(jobIDs, outFields)
  if not result['OK']:
    print result['Message']
    return lfnList
  jobsInfo = [json.loads(info[0]) for info in result['Value']]
  for jobInfo in jobsInfo:
    if 'OutputFileName' in jobInfo:
      lfnList += jobInfo['OutputFileName']

  return lfnList


def filterOutput(lfnList, pattern):
  newList = []

  # convert wildcard to regular expression
  ptemp = pattern.replace('.', '\.')
  ptemp = ptemp.replace('?', '.')
  ptemp = ptemp.replace('*', '.*')
  ptemp = '^' + ptemp + '$'

  for lfn in lfnList:
    m = re.search(ptemp, lfn)
    if m:
      newList.append(lfn)

  return newList


def listOutput(lfnList):
  for lfn in sorted(lfnList):
    print lfn


def downloadCallback(lfn, result):
  status = result['status']
  size = result['size'] if 'size' in result else 0
  span = result['span'] if 'span' in result else 0
  if status not in downloadCounter:
    downloadCounter[status] = 1
  else:
    downloadCounter[status] += 1
  if True or status != 'skip':
    print '%-10s %10s %10s %s' % (status, size, span, os.path.basename(lfn))


def main():
#  method = 'local_rsync'
  method = ['cp', 'local_rsync', 'daemon_rsync', 'dfc']
  listFile = False
  localValidation = True
  outputDir = '.'
  pattern = None
  mergeSize = 0
  removeDownload = True
  useChecksum = False

  for option in options:
    (switch, val) = option
    if switch == 'm' or switch == 'method':
      method = val
    if switch == 'l' or switch == 'list':
      listFile = True
    if switch == 'D' or switch == 'dir':
      outputDir = val
    if switch == 'f' or switch == 'filter':
      pattern = val
    if switch == 'r' or switch == 'reload':
      localValidation = False
    if switch == 'u' or switch == 'checksum':
      useChecksum = True
    if switch == 'g' or switch == 'merge':
      mergeSize = sizeConvert(val)
      if mergeSize < 0:
        print 'ERROR: Invalid merge size format: %s' % val
        return 1
    if switch == 'k' or switch == 'keep':
      removeDownload = False

  if len(args) != 1:
    print 'ERROR: There must be one and only one task ID specified'
    return 1

  try:
    taskID = int(args[0])
  except:
    print 'ERROR: Invalid task ID: %s' % args[0]
    return 1

  lfnList = getTaskOutput(taskID)

  if pattern is not None:
    lfnList = filterOutput(lfnList, pattern)


  if listFile:
    listOutput(lfnList)
    return 0


  # lfnList is ready now
  taskFileNumber = len(lfnList)


  handler = GetOutputHandler(lfnList, method, localValidation, useChecksum)
  realMethod = handler.getMethod()
  gLogger.info('- Using download method:', realMethod)

  remoteFileNumber = handler.getAvailNumber()
  gLogger.always(' Files in the request :', len(lfnList))
  gLogger.always(' Available files      :', remoteFileNumber)

  if remoteFileNumber == 0:
    return 0


  downloadDir = os.path.join(outputDir, 'output_task_%s/download' % taskID)
  if not os.path.exists(downloadDir):
    os.makedirs(downloadDir)

  # Check whether all the remote files are there
  if mergeSize == 0:
    handler.download(downloadDir, downloadCallback)
  else:
    if remoteFileNumber < taskFileNumber:
      s = raw_input('- Task not finished, do you realy want to merge the output files? (y/N) ')
      if s != 'y' and s != 'Y':
        return 0
      gLogger.always('')

    exts = set()
    for lfn in lfnList:
      exts.add(os.path.splitext(lfn)[1])
    exts = list(exts)
    if len(exts) > 1:
      gLogger.error(' Can not merge! Different file types in output:', ', '.join(exts))
      gLogger.error(' Use "-f" option to filter the output files')
      return 0

    ext = exts[0]

    mergeDir = os.path.join(outputDir, 'output_task_%s/merge' % taskID)
    if not os.path.exists(mergeDir):
      os.makedirs(mergeDir)
    else:
      for f in os.listdir(mergeDir):
        if f.endswith(ext):
          os.remove(os.path.join(mergeDir, f))

    handler.downloadAndMerge(downloadDir, mergeDir, 'task_%s_merge' % taskID, ext, mergeSize, removeDownload, downloadCallback)

  print '\n'
  print 'Total:         %s' % len(lfnList)
  print 'Avail:         %s' % remoteFileNumber
  print 'Download:      %s' % len(lfnList)
  print 'Average Speed: %s' % len(lfnList)
  print downloadCounter

#  print Md5CheckSum.checksum('/besfs2/offline/data/665-1/rscan/dst/140101/run_0034530_All_file001_SFO-2.dst')
#  print Adler32CheckSum.checksum('/workfs/bes/zhaoxh/664p03_jpsi_rhopi_stream105_9975_9975_file0001.rec')

  return 0

if __name__ == '__main__':
  if main():
    Script.showHelp()
