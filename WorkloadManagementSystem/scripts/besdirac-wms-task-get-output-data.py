#!/usr/bin/env python

import os
import datetime
import hashlib
import zlib
import json
import re

import pprint

import DIRAC
from DIRAC import S_OK, S_ERROR, exit

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Get all output files of the task

Usage:
   %s [option] TaskID
""" % Script.scriptName )

Script.registerSwitch("m:", "method=",  "Downloading method: local_rsync, dfc, cp, daemon_rsync")
Script.registerSwitch("D:", "dir=",     "Output directory")
Script.registerSwitch("p:", "pattern=", "Output files pattern")
Script.registerSwitch("u",  "checksum", "Use checksum for file validation. Could be slow for some special situations")
Script.registerSwitch("g",  "merge",    "Merge output files")
Script.registerSwitch("r",  "rm",       "Remove downloaded output files after merge")
Script.registerSwitch("z:", "size=",    "Max size of each merged file")

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from BESDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

from BESDIRAC.Badger.private.output.GetOutputHandler   import GetOutputHandler


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
    return progress
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


def main():
#  method = 'local_rsync'
  method = 'cp'
  outputDir = '.'
  pattern = None
  mergeFile = False
  mergeSize = sizeConvert('2G')
  removeDownload = False
  useChecksum = False

  for option in options:
    (switch, val) = option
    if switch == 'm' or switch == 'method':
      method = val
    if switch == 'D' or switch == 'dir':
      outputDir = val
    if switch == 'p' or switch == 'pattern':
      pattern = val
    if switch == 'u' or switch == 'checksum':
      useChecksum = True
    if switch == 'g' or switch == 'merge':
      mergeFile = True
    if switch == 'z' or switch == 'size':
      mergeSize = sizeConvert(val)
      if mergeSize < 0:
        print 'ERROR: Invalid merge size format: %s' % val
        return 1
    if switch == 'r' or switch == 'remove':
      removeDownload = True

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

  handler = GetOutputHandler(method, lfnList, outputDir, useChecksum, mergeFile, mergeSize, removeDownload)

  handler.run()
#  try:
#    handler.run()
#  except Exception as e:
#    print 'ERROR: %s' % e

#  print Md5CheckSum.checksum('/besfs2/offline/data/665-1/rscan/dst/140101/run_0034530_All_file001_SFO-2.dst')
#  print Adler32CheckSum.checksum('/workfs/bes/zhaoxh/664p03_jpsi_rhopi_stream105_9975_9975_file0001.rec')

  return 0

if __name__ == '__main__':
  if main():
    Script.showHelp()
