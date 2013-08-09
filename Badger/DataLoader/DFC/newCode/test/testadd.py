#!/usr/bin/env python

import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

lfn = '/bes/user/z/zhanggang/testadd01'
localfile = './test.py'
se = 'IHEP-USER'
guid = None
result = dirac.addFile(lfn,localfile,se,guid)
