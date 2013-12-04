#!/usr/bin/env python
#############################################
#$HeadURL:$
#data:2013/08/07
#author:gang
#############################################
"""
upload a set of file to SE and register them in DFC.
"""
__RCSID__ = "$Id$"

import time
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch("r","filename","the fullpath of a file")
Script.setUsageMessage('\n'.join([__doc__,
                                'Usage:',
                                '%s dir'% Script.scriptName,
                                'Arguments:'
                                ' dir: dir is a logical directory in DFC']))
Script.parseCommandLine(ignoreErrors=True)
dir = Script.getPositionalArgs()
#print dir
if len(dir)!=1:
    Script.showHelp()

from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()
#localdir = dir[0]
fileName = dir[0]
exitCode = 0
start = time.time()
print "start upload..."
#fileList = badger.getFilenamesByLocaldir(localdir)
fileList = [fileName]
print fileList
result = badger.uploadAndRegisterFiles(fileList)
total = time.time()-start
print "finish,total time is %s"%total
if not result['OK']:
  print 'ERROR %s'%(result['Message'])
  exitCode = 2
DIRAC.exit(exitCode)
  
