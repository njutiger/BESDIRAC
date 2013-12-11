#!/usr/bin/env python
#mtime:2013/12/09
"""
besdirac-dms-dataset-add
  register a new dataset.
  Usage:
    besdirac-dms-dataset-add <datasetName> <path> <conditions>
    Arguments:
      datasetName
      path: the path as the root path
      conditions: meta query conditions
    Example:
      script name1 /zhanggang_test/ "runL>111 runH<200 resonance=jpsi"
"""
__RCSID__ = "$Id$"
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)

args = Script.getPositionalArgs()
print len(args)
if len(args)<3:
  Script.showHelp()
  exit(-1)

datasetName = args[0]
path = args[1]
strArg = args[2]
from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()
badger.registerDataset(datasetName,path,strArg)
exit(0)


