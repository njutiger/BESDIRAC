#-*- coding: utf-8 -*-

from dirac.lib.base import *
from dirac.lib.webBase import defaultRedirect
import dirac.lib.credentials as credentials
from dirac.lib.diset import getRPCClient

from BESDIRAC.TransferSystem.DB.TransferDB import TransRequestEntryWithID

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

    # handle the datetime
    # convert datetime to string
    def quickconv(d):
      from  datetime import datetime
      # submit time is the 5th.
      return dict(zip(TransRequestEntryWithID._fields, tuple(r.strftime("%Y-%m-%d %H:%M [UTC]") if isinstance(r, datetime) else r for r in d )))
    result = map(quickconv, result)

    result_of_reqs["num"] = len(result)
    result_of_reqs["data"] = result

    return result_of_reqs
