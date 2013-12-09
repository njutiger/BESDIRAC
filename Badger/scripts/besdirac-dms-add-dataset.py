#!/usr/bin/env python
# author: zhanggang
"""
besdirac-dms-add-dataset
  Multi-thread upload a set of file to SE and register them in DFC.
  Usage:
    besdirac-dms-add-dataset <Localdir>
    Argument:
      Localdir: the location of files that you want to upload to SE.
    Example:
      script /afs/ihep.ac.cn/users/z/zhanggang/
"""
__RCSID__ = "$Id$"

import time
from DIRAC import S_OK, S_ERROR, gLogger,exit
from DIRAC.Core.Base import Script

Script.registerSwitch("r","dir","the directory that dataset files located")
Script.setUsageMessage(__doc__)
dir = Script.getPositionalArgs()

if len(dir) == 0:
  exit(-1)

from BESDIRAC.Badger.API.Badger import Badger
from BESDIRAC.Badger.API.multiworker import IWorker,MultiWorker

badger = Badger()
localdir = dir[0]
startTime = time.time()
print "startTime",startTime
class UploadWorker(IWorker):
  """ 
  """
  self.errorDict = {}
  def __init__(self, localdir):
    self.m_list = badger.getFilenamesByLocaldir(localdir)
  def get_file_list(self):
    #print self.m_list
    return self.m_list
  def Do(self, item):
    badger.uploadAndRegisterFiles([item])
    if not result['OK']:
      self.errorDict.update(result['Message'])
      print self.errorDict

uw = UploadWorker(localdir)
mw = MultiWorker(uw)
mw.main()
endTime = time.time()-startTime
print "endTime",endTime

exit(1)
