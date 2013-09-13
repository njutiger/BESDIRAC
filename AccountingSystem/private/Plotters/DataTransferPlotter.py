from DIRAC import S_OK, S_ERROR
from BESDIRAC.AccountingSystem.Client.Types.DataTransfer import DataTransfer
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

class DataTransferPlotter( BaseReporter ):
  _typeName = "DataTransfer"
  _typeKeyFields = [ df[0] for df in DataTransfer().definitionKeyFields]
