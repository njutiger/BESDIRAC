# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Get a list of datasets
""")

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Core.DISET.RPCClient import RPCClient

rpc = RPCClient("DataManagement/DatasetFileCatalog")
print rpc.listMetadataSets()
