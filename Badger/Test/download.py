#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC import gLogger,S_OK,S_ERROR


dirac = Dirac()

result = dirac.getFile('/bes/File/jpsi/664/mc/inc2/round05/stream001/664_jpsi_inc2_stream001_run28236_file0004.traw',
    printOutput=True)
print result
