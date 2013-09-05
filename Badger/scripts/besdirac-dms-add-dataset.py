#/usr/bin/env python
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

Script.registerSwitch("r","dir","the directory that dataset files located")
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
localdir = dir[0]
exitCode = 0
start = time.time()
result = badger.uploadAndRegisterFiles(localdir)
total = time.time()-start
print total
if not result['OK']:
  print 'ERROR %s'%(result['Message'])
  exitCode = 2
DIRAC.exit(exitCode)
  
