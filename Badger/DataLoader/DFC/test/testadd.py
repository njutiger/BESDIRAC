#!/usr/bin/env python

import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)
from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

#lfn = '/bes/user/z/zhanggang/testadd01'
#localfile = './test.py'
#
#se = 'IHEP-USER'
#guid = None
#result = dirac.addFile(lfn,localfile,se,guid)


def getFile():
  lfns = ['/bes/user/z/zhanggang/testadd01',
          '/bes/user/z/zhanggang/hello.py',
          '/bes/user/z/zhanggang/dst1.txt']
  result = dirac.getFile(lfns,printOutput=True)
  print result


if __name__=="__main__":
  getFile()
