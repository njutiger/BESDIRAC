#-*- coding: utf-8 -*-

from dirac.lib.base import *
from dirac.lib.webBase import defaultRedirect
import dirac.lib.credentials as credentials
from dirac.lib.diset import getRPCClient

from DIRAC import gLogger

from BESDIRAC.TransferSystem.DB.TransferDB import DatasetEntryWithID
from BESDIRAC.TransferSystem.DB.TransferDB import FilesInDatasetEntryWithID
class DatasetController(BaseController):

  def index(self):
    return render("/transfer/dataset.mako")

  @jsonify
  def datasets(self):
    result_of_reqs = {'num': 0, 'data': []}
    RPC = getRPCClient("Transfer/Dataset")

    cond = {}
    res = RPC.showtotal(cond)

    if not res["OK"]:
      return result_of_reqs
    result_of_reqs["num"] = res["Value"]
    
    # get start/limit
    start = 0
    limit = result_of_reqs["num"]
    orderby = ["id:DESC"]

    if request.params.has_key("start"):
      start = int(request.params["start"])
    if request.params.has_key("limit"):
      limit = int(request.params["limit"])

    res = RPC.show(cond, orderby, start, limit)
    if not res["OK"]:
      gLogger.error(res)
      return result_of_reqs
    result = res["Value"]

    result_of_reqs["data"] = [dict(zip(DatasetEntryWithID._fields, r)) for r in result]

    return result_of_reqs

  @jsonify
  def ls(self):
    result_of_files = {'num': 0, 'data': []}
    if not request.params.has_key("dataset"):
      return result_of_files
    try:
      dataset = str(request.params["dataset"])
    except ValueError:
      return result_of_files

    RPC = getRPCClient("Transfer/Dataset")
    res = RPC.list(dataset)
    if not res["OK"]:
      return result_of_files
    result = res["Value"]

    result_of_files["num"] = len(result)
    result_of_files["data"] = [dict(zip(FilesInDatasetEntryWithID._fields, r)) for r in result]

    return result_of_files
