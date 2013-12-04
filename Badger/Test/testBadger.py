#!/usr/bin/env python
import time
import pprint
import DIRAC
import os
from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)
#from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
args = Script.getPositionalArgs()
strArgs = ' '.join(args)
from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()

#result = badger.getFilesByMetadataQuery('eventType=all bossVer=6.6.3 resonance=4260 runL>=29755 runH<29760 runL!=29756')
result = badger.getFilesByMetadataQuery(strArgs)
pprint.pprint(result)
