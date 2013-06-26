# -*- coding: utf-8 -*-

from dirac.lib.base import *
from dirac.lib.webBase import defaultRedirect
import dirac.lib.credentials as credentials

class ReqsController(BaseController):

  def index(self):
    return render("/transfer/reqs.mako")

  @jsonify
  def getReqs(self):
    data = {}

    data["numofreq"] = 100
    data["reqlist"] = [{"reqid":i} for i in range(100)]
    return data
