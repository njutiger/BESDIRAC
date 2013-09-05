#!/usr/bin/env python
#############################################
#$HeadURL:$
#data:2013/08/07
#author:gang
#############################################
"""
download a set of files as a dataset from SE to the current directory
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch("m","datasetName","the dataset you want to download")
Script.setUsageMessage('\n'.join([__doc__,
                                'Usage:',
                                '%s dir'% Script.scriptName,
                                'Arguments:'
                                ' datasetName: the dataset you want to download']))
Script.parseCommandLine(ignoreErrors=True)
datasetName = Script.getPositionalArgs()
#print dir
if len(datasetName)!=1:
    Script.showHelp()

import time
from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()
exitCode = 0
datasetName = datasetName[0]
start = time.time()
result = badger.downloadFilesByDatasetName(datasetName)
total = time.time()-start
if not result:
  print 'ERROR %s'%(result['Message'])
  exitCode = 1

DIRAC.exit(exitCode)
  
