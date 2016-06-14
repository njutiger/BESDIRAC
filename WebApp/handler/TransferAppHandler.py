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
        # use RPCClient
        transferRequest = RPCClient("Transfer/Dataset")
        condDict = {}
        orderby = []
        start = 0
        limit = 50
        res = transferRequest.showtotal(condDict)
        #self.log.always(res)
        #  {'OK': True, 'rpcStub': (('Transfer/Dataset', {'skipCACheck': False, 'keepAliveLapse': 150, 'delegatedGroup': 'bes_user', 'delegatedDN': '/C=CN/O=HEP/OU=PHYS/O=IHEP/CN=Tian Yan', 'timeout': 600}), 'showtotal', ({},)), 'Value': 68L} 
        limit = res["Value"]

        res = transferRequest.show(condDict, orderby, start, limit)
        #self.log.always(res)
        # {'OK': True, 
        #  'rpcStub': (('Transfer/Dataset', {'delegatedDN': '/C=CN/O=HEP/OU=PHYS/O=IHEP/CN=Tian Yan', 'timeout': 600, 'skipCACheck': False, 'keepAliveLapse': 150, 'delegatedGroup': 'bes_user'}), 'show', ({}, [], 0, 68L)), 
        #  'Value': (
        #            (1L, 'my-dataset', 'lintao'), 
        #            (2L, 'jpsi-test', 'besdirac02.ihep.ac.cn'), 
        #            (3L, 'jpsi-test-10', 'besdirac02.ihep.ac.cn'), 
        #           )
        # } 
        data = []
        if res["OK"]:
            for id, dataset, owner in res["Value"]:
                data.append({
                    "id": id,
                    "owner": owner, 
                    "dataset": dataset,
                })
        self.write({"result": data})
    def web_datasetListFiles(self):
        self.log.debug(self.request.arguments)
        dataset = None
        if self.request.arguments.get("dataset", None):
            dataset = self.request.arguments["dataset"][0]
        data = []
        cache = []
        if dataset:
            RPC = RPCClient("Transfer/Dataset")
            res = RPC.list(dataset)
            #self.log.always(res)
            # {'OK': True, 
            #  'rpcStub': (('Transfer/Dataset', {'skipCACheck': False, 'keepAliveLapse': 150, 'delega tedGroup': 'bes_user', 'delegatedDN': '/C=CN/O=HEP/OU=PHYS/O=IHEP/CN=Tian Yan', 'timeout': 600}), 'list', ('jpsi-all-ok',)), 
            #  'Value': [
            #   (176L, '/zhanggang_test/File/jpsi/6.6.4/mc/inclusive/round02/stream001/jpsi2009_stream001_run10005_file14', 7L), 
            #   (177L, '/zhanggang_test/File/jpsi/6.6.4/mc/inclusive /round02/stream001/jpsi2009_stream001_run10137_file7', 7L), 
            #  ]
            # }
            if res["OK"]:
                cache = res["Value"]


        for i,f,di in cache:
            data.append({
                "id": i,
                "file": f,
            })
        self.write({"result": data})
    # == create ==
    # == delete ==

    # = transfer request (not including file list) =
    # == list ==
    def web_requestList(self):
        # create dummy data
        data = []
        # use RPCClient
        RPC = RPCClient("Transfer/TransferRequest")
        cond = {}
        res = RPC.statustotal(cond)
        #self.log.always(res)
        # {'OK': True, 
        #  'rpcStub': (('Transfer/TransferRequest', {'skipCACheck': False, 'keepAliveLapse': 150, 'delegatedGroup': 'bes_user', 'delegatedDN': '/C=CN/O=HEP/OU=PHYS/O=IHEP/CN=Tian Yan', 'timeout': 600}), 'statustotal', ({},)),
        #  'Value': 159L}
        start = 0
        limit = res["Value"]
        orderby = ["id:DESC"]
        cache = []
        res = RPC.statuslimit(cond, orderby, start, limit)
        if res["OK"]:
            cache = res["Value"]
        #self.log.always(cache)
        # (159L, 
        #  'yant', 
        #  'prod_rtg_r04_add_150104', 
        #  'IHEPD-USER', 
        #  'WHU-USER', 
        #  'DIRACDMS', 
        #  datetime.datetime(2015, 6, 12, 8, 41, 19), 
        #  'finish'),

        for vv in cache:
            data.append({
                "id": vv[0],
                "owner": vv[1], 
                "dataset": vv[2],
                "srcSE": vv[3],
                "dstSE": vv[4],
                "protocol": vv[5], 
                "submitTime": vv[6].strftime("%Y-%m-%d %H:%M [UTC]") if vv[6] else "",
                "status": vv[7],
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
        self.log.debug(self.request.arguments)
        valid_list = ["dataset", "srcse", "dstse", "protocol"]
        build_input_param = {}
        for k in valid_list:
            if not self.request.arguments.has_key(k):
                raise WErr( 400, "Missing %s" % k )
            build_input_param[k] = self.request.arguments[k][0]
        self.log.debug(build_input_param)
        # check the data
        ## SE
        if build_input_param["dstse"] == build_input_param["srcse"]:
            raise WErr( 400, "dstse and srcse are same" )
        ## protocol
        if build_input_param["protocol"] not in ["DIRACDMS", "DIRACFTS"]:
            raise WErr( 400, "protocol %s is wrong"%build_input_param["protocol"] )

        self.set_status(200)
        self.finish()
        self.log.debug("finish")

    # = file list of one request =
    # == list ==
    def web_requestListFiles(self):
        self.log.debug(self.request.arguments)
        cache = []
        if self.request.arguments.get("reqid", None):
            reqid = int(self.request.arguments["reqid"][0])
            RPC = RPCClient("Transfer/TransferRequest")
            cond = {"trans_req_id": reqid}
            res = RPC.show(cond)
            if res["OK"]:
                cache = res["Value"]
        #self.log.always(cache)
        # (1L, 
        #  '/path/does/not/exist', 
        #  1L, # datasetid
        #  datetime.datetime(2013, 8, 23, 3, 12, 37), 
        #  datetime.datetime(2013, 8, 23, 3, 14, 37), 
        #  'finish', 
        #  'error'
        # )
        data = []
        for vv in cache:
            data.append({
                "id": vv[0],
                "LFN": vv[1],
                "starttime": vv[3].strftime("%Y-%m-%d %H:%M [UTC]") if vv[3] else "",
                "finishtime": vv[4].strftime("%Y-%m-%d %H:%M [UTC]") if vv[4] else "",
                "status": vv[5],
                "error": vv[6],
            })
        self.write({"result": data})


