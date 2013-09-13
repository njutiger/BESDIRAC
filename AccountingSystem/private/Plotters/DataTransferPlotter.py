from DIRAC import S_OK, S_ERROR
from BESDIRAC.AccountingSystem.Client.Types.DataTransfer import DataTransfer
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter

class DataTransferPlotter( BaseReporter ):
  _typeName = "DataTransfer"
  _typeKeyFields = [ df[0] for df in DataTransfer().definitionKeyFields]

  def _reportDataTransfered( self, reportRequest ):
    selectFields = ( self._getSelectStringForGrouping( reportRequest[ 'groupingFields' ] ) + ", SUM(%s)",
                     reportRequest[ 'groupingFields' ][1] + [ 'TransferSize' ]
                   )
    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                reportRequest[ 'groupingFields' ],
                                {} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = retVal[ 'Value' ]
    for key in dataDict:
      dataDict[ key ] = int( dataDict[ key ] )
    return S_OK( { 'data' : dataDict  } )

  def _plotDataTransfered( self, reportRequest, plotInfo, filename ):
    metadata = { 'title' : 'Total data transfered by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'bytes',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )

