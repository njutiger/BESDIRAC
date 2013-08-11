# -*- coding: utf-8 -*-

import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/TransferRequest")

condDict = {}
res = transferRequest.statuslimit(condDict, ["id:DESC"], 5, 10)
if res["OK"]:
  for line in res["Value"]:
    print line
else:
  print res["Message"]
