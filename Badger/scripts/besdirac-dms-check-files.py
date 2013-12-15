#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang
'''checksum,compare the size of LFN files and Local files 
   Usage :
    besdirac-dms-check-files <dfcDir> <localDir>
    Example: besdirac-dms-check-files /dir1  /dir2
'''
import os.path
import pprint
import DIRAC
from DIRAC.Core.Base import Script

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=True)
from BESDIRAC.Badger.API.Badger import Badger

badger = Badger()
dirs = Script.getPositionalArgs()
dir = dirs[0]
localDir = dirs[1]

lfns = badger.listDir(dir)
lfnDict = badger.getSize(lfns)

localFiles = badger.getFilenamesByLocaldir(localDir)
localDict = {}
for file in localFiles:
  localDict[file] = os.path.getsize(file)

filesOK = True
omitList = []
partList = []
for item in lfnDict.keys():
  afsitem = os.path.join(localDir,os.path.basename(item))
  if afsitem in localDict.keys():
    if localDict[afsitem]!=lfnDict[item]:
      partList.append(item,localDict[afsitem],lfnDict[item])
      filesOK = False
    else:
      pass
  else:
    omitList.append(item)
if partList:
  print "these file has not transfer completely"
  print partList
if omitList:
  print "these file has not tranfer yet."
  print omitList
if filesOK:
  print "all are OK"

DIRAC.exit(0)
