#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang
"""
multi-thread download a set of file to SE and register them in DFC.
"""
__RCSID__ = "$Id$"

import time
import DIRAC
from DIRAC.Core.Base import Script
Script.registerSwitch("r","dir","the directory that dataset files located")
Script.setUsageMessage('\n'.join([__doc__,
                                'Usage:',
                                '%s'% Script.scriptName,
                                'Arguments:'
                                ' dir:is a logical directory in DFC']))
Script.parseCommandLine(ignoreErrors=True)

args = Script.getPositionalArgs()


if len(args) == 0:
  Script.showHelp()
  DIRAC.exit(-1)
destDir = ''
datasetName = args[0]
if len(args) > 1:
  destDir = args[1]

from BESDIRAC.Badger.API.Badger import Badger
from BESDIRAC.Badger.API.multiworker import IWorker,MultiWorker
badger = Badger()
print "start downloading dataset %s..."%datasetName
start = time.time()
class DownloadWorker(IWorker):
  """ """
  #if file failed download,then append in errorDict

  errorDict = {}
  def __init__(self, datasetName):
    self.m_list = badger.getFilesByDatasetName(datasetName)
  def get_file_list(self):
    return self.m_list
  def Do(self, item):
    result = badger.downloadFilesByDatasetName([item],destDir)
    if not result['OK']:
      #print result['Message'],type(result['Message'])
      self.errorDict.update(result['errorDict'])
      #print self.errorDict


dw = DownloadWorker(datasetName)
mw = MultiWorker(dw,1)
mw.main()
total=time.time()-start
print "Finished,total time is %s"%total

#print "Some failed file %s"%errorDict
exitCode = 1
DIRAC.exit(exitCode)

