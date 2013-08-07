#-*- coding: utf-8 -*-

from dirac.lib.base import *
from dirac.lib.webBase import defaultRedirect
import dirac.lib.credentials as credentials
from dirac.lib.diset import getRPCClient

class MonitorController(BaseController):

  def index(self):
    return render("/transfer/monitor.mako")

  @jsonify
  def reqs(self):
    result_of_reqs = {'num': 0, 'data': []}
    #username = str(credentials.getUsername())
    RPC = getRPCClient("Transfer/TransferRequest")
    cond = {}
    res = RPC.status(cond)
    if not res["OK"]:
      return result_of_reqs
    result = res["Value"]
    result_of_reqs["num"] = len(result)
    result_of_reqs["data"] = result

    return result_of_reqs
