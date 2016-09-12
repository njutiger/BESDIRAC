#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: lintao

import subprocess

import DIRAC
from DIRAC import gLogger

class TransferFactory(object):
  PROTOCOL = ["DIRACFTS", "DIRACDMS"]

  def generate(self, protocol, info):
    gLogger.info("Load Module:")
    gLogger.info("BESDIRAC.TransferSystem.Agent.helper.TransferFactory.TransferBy%s"%(protocol))
    mod = __import__("BESDIRAC.TransferSystem.Agent.helper.TransferFactory.TransferBy%s"%(protocol),
        globals(),
        locals(),
        ["%sTransferWorker"%(protocol)]
        )
    TR = getattr(mod, "%sTransferWorker"%(protocol))
    tr = TR()
    tr.create_popen(info)
    return tr


gTransferFactory = TransferFactory()
