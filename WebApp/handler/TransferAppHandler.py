#!/usr/bin/env python
#-*- coding: utf-8 -*-

from WebAppDIRAC.Lib.WebHandler import WebHandler
from DIRAC.Core.DISET.RPCClient import RPCClient

class TransferAppHandler(WebHandler):
    AUTH_PROPS = "authenticated"

    # = dataset management =
    # == list ==
    def web_datasetList(self):
        # create dummy data
        data = []
        for i in range(10):
            data.append({
                "id": i,
                "owner": "lintao", 
                "dataset": "lintao%d"%i,
            })
        self.write({"result": data})
    # == create ==
    # == delete ==

    # = transfer request (not including file list) =
    # == list ==

    # = file list of one request =
    # == list ==


