#!/usr/bin/env python
#mtime:2013/12/10
"""
besdirac-dms-get-files
  This script get a set of files from SE to localdir.

  Usage:
    besdirac-dms-dataset-get datasetName 
    Arguments:
      datasetName: a dataset that contain a set of files.
    Examples:
      besdirac-dms-dataset-get User_XXX_XXX 
"""
__RCSID__ = "$Id$"

import os
import anydbm
import time
import tempfile
import subprocess
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script


Script.setUsageMessage(__doc__)
Script.registerSwitch("m:", "method=", "Downloading method")
Script.registerSwitch("D:", "dir=",    "Output directory")
Script.registerSwitch("w:", "wait=",   "Waiting interval (s)")
Script.registerSwitch("t:", "thread=", "Simultaneously downloading thread number")

Script.parseCommandLine(ignoreErrors=True)
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()
if not args:
  Script.showHelp()
  exit(1)
setName = args[0]

method = 'rsync'
output_dir = '.'
interval = 300
for option in options:
  (switch, val) = option
  if switch == 'm' or switch == 'method':
    method = val
  if switch == 'D' or switch == 'dir':
    output_dir = val
  if switch == 'w' or switch == 'wait':
    interval = int(val)

from BESDIRAC.Badger.API.Badger import Badger
from BESDIRAC.Badger.API.multiworker import IWorker,MultiWorker

def getDB(name,function):
  """return a db instance,the db contain the file list.
  default value is 0,means the file is not transfer yet,if 2,means OK.
  """
  dbname = "db_"+name[-4:]
  if not os.path.exists(dbname):
    result = function(name)
    if result['OK']:
      fileList = result['Value'] 
      db = anydbm.open(dbname,'c')
      for file in fileList:
       db[file] = '0'
       db.sync()
  else:
    db = anydbm.open(dbname,'c')
  return (db,dbname)

class DownloadWorker(IWorker):
  """
  """
  #if file failed download,then append in errorDict
  #self.errorDict = {}

  def __init__(self):
    self.badger = Badger()
    self.db,self.dbName = getDB(setName,self.badger.getFilesByDatasetName)
    print self.db,self.dbName

  def get_file_list(self):
    #return self.m_list
    for k,v in self.db.iteritems():
      if v=='2':
        continue
      yield k
      #print k

  def Do(self, item):
    badger = Badger()
    #print "world"
    result = badger.downloadFilesByFilelist([item])#,destDir)
    #print "result",result
    if result['OK']:
      self.db[item] = '2'
      self.db.sync()
  def Clear(self):
    transferOK = True
    for k,v in self.db.iteritems():
      if v=='0':
        transferOK = False
        print "Some files failed, you need run this script again"
        break
    self.db.close()
    if transferOK:
      print "All files transfer successful"
      os.remove(self.dbName)

def datasetGet():
  print "start download..."
  start = time.time()

  dw = DownloadWorker()
  mw = MultiWorker(dw,5)
  mw.main()
  dw.Clear()
  total=time.time()-start
  print "Finished,total time is %s"%total

class Rsync:
  def __init__(self):
    self.listFile = tempfile.NamedTemporaryFile(mode='w', prefix='tmp_filelist_', delete=False)
    self.dirName = ''

  def __del__(self):
    self.listFile.close()
    os.remove(self.listFile.name)

  def getFileList(self):
    badger = Badger()
    result = badger.getFilesByDatasetName(setName)
    if result['OK']:
      fileList = result['Value']
      if fileList:
        self.dirName = os.path.dirname(fileList[0])
      for file in fileList:
        print >>self.listFile, os.path.basename(file)
    self.readyNum = len(fileList)
    self.listFile.close()
    print 'There are %s files ready for download' % self.readyNum

  def sync(self):
    cmd = ["rsync", "-avvvz", "--partial", "--files-from=%s"%self.listFile.name, "rsync://bws0629.ihep.ac.cn:8873/bes-srm%s"%self.dirName, "%s"%output_dir]
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    self.totalNum = 0
    self.downloadNum = 0
    self.skipNum = 0
    for line in iter(popen.stdout.readline, ""):
      if line.find('recv_file_name') != -1:
        self.totalNum += 1
      if line.find('renaming') != -1:
        self.downloadNum += 1
        print 'File downloaded: %s' % line.split()[-1]
      if line.find('is uptodate') != -1:
        self.skipNum += 1
      if line.find('bytes/sec') != -1:
        self.speed = float(line.split()[-2]) / 1024 / 1024
    self.status = popen.wait()

  def output(self):
    print 'Download status: %s, speed: %.2f (MB/s)' % (self.status, self.speed)
    print 'Generated: %s\nTotal: %s. Download: %s. Skip: %s' % (self.readyNum, self.totalNum, self.downloadNum, self.skipNum)


def datasetRsync():
  while True:
    rsync = Rsync()
    rsync.getFileList()
    rsync.sync()
    rsync.output()
    del rsync
    print 'Waiting %s seconds for next downloading... Press Ctrl+C to exit\n' % interval
    time.sleep(interval)

if method == 'get':
  datasetGet()
elif method == 'rsync':
  datasetRsync()

exit(0)
