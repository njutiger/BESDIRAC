#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang
"""
multi-thread upload a set of file to SE and register them in DFC.
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
dir = Script.getPositionalArgs()

if len(dir) == 0:
  DIRAC.exit(-1)

from BESDIRAC.Badger.API.Badger import Badger
from BESDIRAC.Badger.API.multiworker import IWorker,MultiWorker
badger = Badger()
localdir = dir[0]
startTime = time.time()
print "startTime",startTime
class UploadWorker(IWorker):
  """ """
  def __init__(self, localdir):
    self.m_list = badger.getFilenamesByLocaldir(localdir)
  def get_file_list(self):
    #print self.m_list
    return self.m_list
  def Do(self, item):
    badger.uploadAndRegisterFiles([item])


uw = UploadWorker(localdir)
mw = MultiWorker(uw)
mw.main()
endTime = time.time()-startTime
print "endTime",endTime
