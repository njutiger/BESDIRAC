#-*- coding: utf-8 -*-

from dirac.controllers.systems.accountingPlots import AccountingplotsController

class AcctController(AccountingplotsController):

  def trans(self):
    return self.__showPlotPage( "Transfer", "/transfer/acct.mako" )
