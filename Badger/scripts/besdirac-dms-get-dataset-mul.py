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
                                '%s dir'% Script.scriptName,
                                'Arguments:'
                                ' dir: dir is a logical directory in DFC']))
Script.parseCommandLine(ignoreErrors=True)
datasetName = Script.getPositionalArgs()


if len(datasetName)!=1:
  Script.showHelp()
  DIRAC.exit(-1)

from BESDIRAC.Badger.API.Badger import Badger
from BESDIRAC.Badger.API.multiworker import IWorker,MultiWorker
badger = Badger()
datasetName = datasetName[0]
print "start downloading dataset %s"%datasetName
start = time.time()
class DownloadWorker(IWorker):
  """ """
  def __init__(self, datasetName):
    self.m_list = badger.getFilesByDatasetName(datasetName)
  def get_file_list(self):
    return self.m_list
  def Do(self, item):
    badger.downloadFilesByDatasetName([item])


dw = DownloadWorker(datasetName)
mw = MultiWorker(dw,10)
mw.main()
total=time.time()-start
print total

