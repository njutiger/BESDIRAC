#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang
"""
list dataste names and their metadata
"""
__RCSID__ = "$Id$"

import time
import pprint
import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)
Script.setUsageMessage('\n'.join([__doc__,
                                'Usage:',
                                '%s dir'% Script.scriptName,
                                'Arguments:'
                                ' dir: dir is a logical directory in DFC']))

from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()
pprint.pprint(badger.listDatasets())
exitCode = 1
DIRAC.exit(exitCode)

