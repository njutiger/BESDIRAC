#/usr/bin/env python
#############################################
#$HeadURL:$
#data:2013/08/07
#author:gang
#############################################
"""
Regrieve a set of files as a dataset from SE to th current directory
"""
__RCSID__ = "$Id$"

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

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
client = FileCatalogClient()
from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()
exitCode = 0

dir = dir[0]
lfns = []
result = client.listDirectory(dir)
if result['OK']:
  for lfn in result['Value']['Successful'][dir]['Files']:
    lfns.append(lfn)
  result = dirac.getFile(lfns,printOutput=True)
  if not result['OK']:
    print 'ERROR %s'%(result['Message'])
    exitCode = 2

else:
  exitCode = 1 
DIRAC.exit(exitCode)
  
