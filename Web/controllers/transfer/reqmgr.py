#-*- coding: utf-8 -*-

from dirac.lib.base import *
from dirac.lib.webBase import defaultRedirect
import dirac.lib.credentials as credentials
from dirac.lib.diset import getRPCClient

from DIRAC import S_OK, S_ERROR

class ReqmgrController(BaseController):

  @jsonify
  def create(self):
    if not request.params.has_key("dataset"):
      return S_ERROR("Lack Dataset Name")
    if not request.params.has_key("src_se"):
      return S_ERROR("Lack SRC SE Name")
    if not request.params.has_key("dst_se"):
      return S_ERROR("Lack DST SE Name")
    if not request.params.has_key("protocol"):
      return S_ERROR("Lack Transfer Protocol")
    RPC = getRPCClient("Transfer/TransferRequest")

    return RPC.create(request.params["dataset"].encode("utf-8"),
                      request.params["src_se"].encode("utf-8"),
                      request.params["dst_se"].encode("utf-8"),
                      request.params["protocol"].encode("utf-8"))

  @jsonify
  def modify(self):
    pass

  @jsonify
  def delete(self):
    """
      This is to delete the file tranfer in one request.
      We don't support delete the whole request now.
    """
    if not request.params.has_key("id"):
      return S_ERROR("Lack File ID")
    try:
      fileid = int(request.params["id"].encode("utf-8"))
    except Exception as e:
      return S_ERROR(str(e))
    RPC = getRPCClient("Transfer/TransferRequest")
    return RPC.delete({"id":fileid})

  @jsonify
  def retransfer(self):
    """
      This is to retransfer the file tranfer in one request.
    """
    if not request.params.has_key("id"):
      return S_ERROR("Lack File ID")
    try:
      fileid = int(request.params["id"].encode("utf-8"))
    except Exception as e:
      return S_ERROR(str(e))
    RPC = getRPCClient("Transfer/TransferRequest")
    return RPC.retransfer({"id":fileid})
