#!/usr/bin/env python
#-*- coding: utf-8 -*-

from WebAppDIRAC.Lib.WebHandler import WebHandler, WErr
from DIRAC.Core.DISET.RPCClient import RPCClient
import datetime

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
    def web_datasetListFiles(self):
        self.log.debug(self.request.arguments)
        value = 0
        if self.request.arguments.get("datasetid", None):
            value = int(self.request.arguments["datasetid"][0])
        data = []
        for i in range(value):
            data.append({
                "id": i,
                "file": "/p/%d"%i,
            })
        self.write({"result": data})
    # == create ==
    # == delete ==

    # = transfer request (not including file list) =
    # == list ==
    def web_requestList(self):
        # create dummy data
        data = []
        for i in range(10):
            data.append({
                "id": i,
                "owner": "lintao", 
                "dataset": "lintao%d"%i,
                "srcSE": "IHEP-USER",
                "dstSE": "UCAS-USER",
                "protocol": "DMS", 
                "submitTime": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M [UTC]"),
                "status": "OK",
            })
        self.write({"result": data})

    # == new ==
    # create a new transfer request
    def web_requestNew(self):
        # validate
        # * dataset
        # * srcse
        # * dstse
        # * protocol
        valid_list = ["dataset", "srcse", "dstse", "protocol"]
        build_input_param = {}
        for k in valid_list:
            if not self.request.arguments.has_key(k):
                raise WErr( 400, "Missing %s" % k )
            build_input_param[k] = self.request.arguments[k]
        self.set_status(200)
        self.finish()

    # = file list of one request =
    # == list ==
    def web_requestListFiles(self):
        self.log.debug(self.request.arguments)
        value = 0
        if self.request.arguments.get("reqid", None):
            value = int(self.request.arguments["reqid"][0])
        data = []
        for i in range(value):
            data.append({
                "id": i,
                "LFN": "/p/%d"%i,
                "starttime": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M [UTC]"),
                "finishtime": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M [UTC]"),
                "status": "OK",
                "error": "",
            })
        self.write({"result": data})


