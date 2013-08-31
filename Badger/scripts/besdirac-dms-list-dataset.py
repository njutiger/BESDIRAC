#!/usr/bin/env python

"""
List the Files in the Dataset (in DFC)
"""

import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine(ignoreErrors=True)

datasets = Script.getPositionalArgs()
if len(datasets)==0:
  DIRAC.exit(-1)

from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()

for ds in datasets:
  result = badger.getFilesByDatasetName(ds)
  if result and isinstance(result, list):
    for lfn in result:
      print lfn
