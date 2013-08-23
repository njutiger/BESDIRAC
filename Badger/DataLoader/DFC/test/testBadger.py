#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors = True)
from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()

result = badger.testFunction()
print result
