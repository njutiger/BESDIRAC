#-*- coding: utf-8 -*-

from dirac.lib.base import *
from dirac.lib.webBase import defaultRedirect
import dirac.lib.credentials as credentials

class InfoController(BaseController):

  def index(self):
    return render("/transfer/info.mako")

  @jsonify
  def getInfoList(self):
    realdata = [
        {"FuncName":"ListRequest", "ScriptName":"bes-dirac-transfer-list-request.py"},
        {"FuncName":"ListFilelist", "ScriptName":"bes-dirac-transfer-list-files.py"},
        {"FuncName":"Status", "ScriptName":"bes-dirac-transfer-status.py"} 
        ]
    data = { 'numRecords': len(realdata),
             'functions': realdata}
    return data

  @jsonify
  def getDetailList(self):
    detail_data = {
      "ListRequest":{"author":"lintao", "detail":"xxx"},
      "ListFilelist":{"author":"lintao", "detail":"yyy"},
      "Status":{"author":"lintao", "detail":"zzz"},
    }
    name = ""
    if request.params.has_key("funcname"):
      name = request.params["funcname"]

    if name in detail_data:
      return {'num': 10*len(name), 'data': [detail_data[name]]*(10*len(name))}
    return {'num':0, 'data': []}


