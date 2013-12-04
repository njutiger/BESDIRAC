#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang
"""
find all files statusfying th given metadata information
usage script <destDir> <meta_name>=<meta_value> [<meta_name>=<meta_value>]
"""
__RCSID__ = "$Id$"

import time
import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__,
                                'Usage:',
                                '%s'% Script.scriptName,
                                'Arguments:'
                                ]))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) == 0 or len(args)==1:
  Script.showHelp()
  DIRAC.exit(-1)
destDir = args[0]
del args[0]
strArg = ' '.join(args)
from BESDIRAC.Badger.API.Badger import Badger
from BESDIRAC.Badger.API.multiworker import IWorker,MultiWorker

print "start downloading ..."
start = time.time()
class DownloadWorker(IWorker):
  """ """
  #if file failed download,then append in errorDict
  errorDict = {}
  def __init__(self, strArg):
    self.badger = Badger()
    self.m_list = self.badger.getFilesByMetadataQuery(strArg)
  def get_file_list(self):
    return self.m_list
  def Do(self, item):
    badger = Badger()
    result = badger.downloadFilesByDatasetName([item],destDir)
    if not result['OK']:
      #print result['Message'],type(result['Message'])
      self.errorDict.update(result['errorDict'])
      #print self.errorDict


dw = DownloadWorker(strArg)
#print dw.get_file_list()
mw = MultiWorker(dw,5)
mw.main()
total=time.time()-start
print "Finished,total time is %s"%total

#print "Some failed file %s"%errorDict
exitCode = 1
DIRAC.exit(exitCode)

