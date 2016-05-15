# $HeadURL $
""" PublisherHandler

  PublisherHandler for BESDIRAC

"""

from datetime import datetime
from types    import NoneType

# DIRAC
from DIRAC                                                      import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.ResourceStatusSystem.Service.PublisherHandler import PublisherHandler as DIRACPublisherHandler
from BESDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id: $'

# RSS Clients
rmClient  = None

def initializePublisherHandler( _serviceInfo ):
  """
  Handler initialization in the usual horrible way.
  """
  
  global rmClient 
  rmClient = ResourceManagementClient()
  
  return S_OK()
  
class PublisherHandler( DIRACPublisherHandler ):
  """
  PublisherHandler inherits from DIRAC's PublisherHandler.
  """  

  def __init__( self, *args, **kwargs ):
    """
    Constructor
    """
    super( PublisherHandler, self ).__init__( *args, **kwargs )
  
  # My Methods ...................................................
  
  types_getVOs = []
  def export_getVOs( self ):
    """
    Returns the list of all VOs.
    """
    
    gLogger.info( 'getVOs' )
    
    return gConfig.getSections( 'Registry/VO' )
  
  types_getDomains = []
  def export_getDomains( self ):
    """
    Returns the list of all site domains.
    """
    
    gLogger.info( 'getDomains' )
    
    return gConfig.getSections( 'Resources/Sites' )
  
  types_getSiteVO = [ str ]
  def export_getSiteVO( self, siteName ):
    """
    Returns the VO for the given site.
    """

    gLogger.info( 'getSiteVO' )
    
    return CSHelpers.getSiteVO( siteName )
  
  types_getSiteResource = [ str ]
  def export_getSiteResource( self, siteName ):
    """
    Returns the dictionary with CEs and SEs for the given site.
    
    :return: S_OK( { 'ComputingElement' : celist, 'StorageElement' : selist } ) | S_ERROR
    """
    
    gLogger.info( 'getSiteResource' )
    
    siteType = siteName.split( '.' )[ 0 ]
      
    if siteType == 'CLOUD':
      ces = []
    else:
      ces = CSHelpers.getSiteComputingElements(siteName)
          
    ses = CSHelpers.getSiteStorageElements( siteName )
      
    return S_OK( { 'ComputingElement' : ces, 'StorageElement' : ses } )
  
  types_getSiteSummary = [ ( str, NoneType, list ) ]
  def export_getSiteSummary( self, siteNames ):
    """
    Return the dictionary with summary information for the given sites.
    
    :return: S_OK( { site : { 'CEStatus' : 
                                                   'SEStatus' : 
                                                   'SEOccupied' : 
                                                   'Efficiency' : 
                                                   'Running' :
                                                   'Waiting' :
                                                   'Done' :
                                                   'Failed' :
                                                   'Completed ' :
                                                   'Stalled' : } } ) | S_ERROR
    """
    
    gLogger.info( 'getSiteSummary' )
    
    samSummary = {}
    for siteName in siteNames:
      samSummary[ siteName ] = {}
      
    ses = CSHelpers.getStorageElements()
    if not ses[ 'OK' ]:
      return ses
    ses = ses[ 'Value' ]
    queryRes = rmClient.selectStorageCache( sE = ses )
    if not queryRes[ 'OK' ]:
      return queryRes
    records = queryRes[ 'Value' ]
    columns = queryRes[ 'Columns' ]
      
    seInfo = {}
      
    for record in records:
      seDict = dict( zip( columns, record ) )
      seName = seDict.pop( 'SE' )
      seInfo[ seName ] = { 'SEOccupied' : seDict[ 'Occupied' ] }
          
    queryRes = rmClient.selectSiteSAMStatus( site = siteNames )
    if not queryRes[ 'OK' ]:
      return queryRes
    records = queryRes[ 'Value' ]
    columns = queryRes[ 'Columns' ]
    
    siteSAMStatus = {}
    
    for record in records:
      samDict = dict( zip( columns, record ) )
      siteName = samDict[ 'Site' ]
      siteSAMStatus[ siteName ] = { 'CEStatus' : samDict[ 'CEStatus' ], 'SEStatus' : samDict[ 'SEStatus' ] }
 
    queryRes = rmClient.selectJobCache( site = siteNames )
    if not queryRes[ 'OK' ]:
      return queryRes
    records = queryRes[ 'Value' ]
    columns = queryRes[ 'Columns' ]
      
    siteJob = {}
      
    for record in records:
      jobDict = dict( zip( columns, record ) )
      siteName = jobDict[ 'Site' ]
      siteJob[ siteName ] = {}
      siteJob[ siteName ][ 'Running' ] = jobDict[ 'Running' ]
      siteJob[ siteName ][ 'Waiting' ] = jobDict[ 'Waiting' ]
      siteJob[ siteName ][ 'Done' ] = jobDict[ 'Done' ]
      siteJob[ siteName ][ 'Failed' ] = jobDict[ 'Failed' ]
      siteJob[ siteName ][ 'Completed' ] = jobDict[ 'Completed' ]
      siteJob[ siteName ][ 'Stalled' ] = jobDict[ 'Stalled' ]
      siteJob[ siteName ][ 'Efficiency' ] = jobDict[ 'Efficiency' ]
            
    siteSummary = {}
      
    for siteName in siteNames:
      siteSummary[ siteName ] = {}
          
      if siteSAMStatus.has_key( siteName ):
        siteSummary[ siteName ].update( siteSAMStatus[ siteName ] )
      else:
        siteSummary[ siteName ].update( { 'CEStatus' : '', 'SEStatus' : '' } )
              
      if siteJob.has_key( siteName ):
        siteSummary[ siteName ].update( siteJob[ siteName ] )
      else:
        siteSummary[ siteName ].update( { 'Running' : 0, 'Waiting' : 0, 'Done' : 0, 'Failed' : 0, 'Completed' : 0, 'Stalled' : 0, 'Efficiency' : 0.0 } )
        
      siteSE = CSHelpers.getSiteStorageElements( siteName )
      if siteSE:
        siteSummary[ siteName ].update( seInfo[ siteSE[ 0 ] ] )
      else:
        siteSummary[ siteName ].update( { 'SEOccupied' : '' } )
              
    return S_OK( siteSummary )
  
  types_getHostEfficiency = [ str ]
  def export_getHostEfficiency( self, siteName ):
    """
    Retruns the job efficiency for hosts of the given site. 
    
    :return: S_OK( [ { 'HostName' :
                                        'Running' :
                                        'Done' :
                                        'Failed' :
                                        'Efficiency' : } ] ) / S_ERROR
    """
    
    gLogger.info( 'getHostEfficiency' )
      
    sql = """
select JP.Value, J.Status, count(*) from Jobs J, JobParameters JP 
where J.JobID = JP.JobID and J.Site = '%s' and JP.Name = 'HostName' 
and J.LastUpdateTime >= DATE_SUB(UTC_TIMESTAMP(),INTERVAL 24 HOUR) 
group by JP.Value, J.Status
""" % siteName

    jobDB = JobDB()
    queryRes =  jobDB._query( sql )
    if not queryRes[ 'OK' ]:
      return queryRes
    records = queryRes[ 'Value' ]
      
    hostInfo = {}
    for record in records:
      hostName = record[ 0 ] 
      if not hostInfo.has_key( hostName ):
        hostInfo[ hostName ] = { 'Running' : 0, 'Done' : 0, 'Failed' : 0, 'Efficiency' : 0.0 }
      hostInfo[ hostName ][ record[ 1 ] ] = record[ 2 ]
          
    results = []
    for hostName, hostDict in hostInfo.items():
      hostDict[ 'Host' ] = hostName
      if hostDict[ 'Done' ] != 0 or hostDict[ 'Failed' ] != 0:
        hostDict[ 'Efficiency' ] = hostDict[ 'Done' ] / ( hostDict[ 'Done' ] + hostDict[ 'Failed' ] )
      results.append( hostDict )
          
    return S_OK( results )
     
  
  types_getSAMDetail = [ str, str ]
  def export_getSAMDetail( self, elementName, testType ):
    """
    Returns the dictionary with SAM test detail information for the given test.
    """
    
    gLogger.info( 'getSAMDetail' )
    
    queryRes = rmClient.selectSAMResult( elementName = elementName, testType = testType )
    if not queryRes[ 'OK' ]:
      return queryRes
    record = queryRes[ 'Value' ][ 0 ]
    columns = queryRes[ 'Columns' ]
      
    detail = dict( zip( columns, record ) )
    detail.pop( 'LastCheckTime' )
    return S_OK( detail )
      
      
  types_getSAMSummary = [ str ]
  def export_getSAMSummary( self, siteName ):
    """
    Returns SAM tests status for the elements of the given site.
    
    :return: S_OK( { element : { 'ElementType' :
                                                            'WMSTest' : 
                                                            'CVMFSTest' : 
                                                            'BOSSTest' :
                                                            'SETest' : } } ) / S_ERROR
    
    """
    
    gLogger.info( 'getSAMSummary' )
    
    siteType = siteName.split( '.' )[ 0 ]
    if 'CLOUD' == siteType:
      ces = [ siteName ]
    else:
      ces = CSHelpers.getSiteComputingElements( siteName )
    ses = CSHelpers.getSiteStorageElements( siteName )
      
    queryRes = rmClient.selectSAMResult( elementName = ces + ses )
    if not queryRes[ 'OK' ]:
      return queryRes
    records = queryRes[ 'Value' ]
    columns = queryRes[ 'Columns' ]
      
    samSummary = {}
    for ce in ces:
      samSummary[ ce ] = { 'ElementType' : 'ComputingElement' }
    for se in ses:
      samSummary[ se ] = { 'ElementType' : 'StorageElement' }
    for record in records:
      samDict = dict( zip( columns, record ) )
      elementName = samDict[ 'ElementName' ]
      testType = samDict[ 'TestType' ]
      status = samDict[ 'Status' ]
      samSummary[ elementName ][ testType ] = status
              
    return S_OK( samSummary )
  
  
#   types_getSitesSAMHistoryWeb = [ ( str, NoneType, list ), datetime, datetime ]
#   def export_getSitesSAMHistoryWeb( self, siteNames, fromDate, toDate ):
#     gLogger.info( 'getSitesSAMHistoryWeb' )
#     if fromDate > toDate:
#       return S_ERROR( 'fromDate > toDate.' )
#     
#     samHistory = {}
#     for siteName in siteNames:
#       samHistory[ siteName ] = []
#     
#     fromDate = datetime.strftime( fromDate, '%Y-%m-%d %H:%M:%S' )
#     toDate = datetime.strftime( toDate, '%Y-%m-%d %H:%M:%S' )
#     queryRes = rmClient.selectSiteSAMStatusLog(
#                                                    site = siteNames,
#                                                    meta = { 'newer' : ['LastCheckTime', fromDate ],
#                                                                     'older' : [ 'LastCheckTime', toDate ],
#                                                                     'columns' : [ 'Site', 'Status', 'LastCheckTime' ] }
#                                                    )
#     if not queryRes[ 'OK' ]:
#       return queryRes
#     records = list( queryRes[ 'Value' ] )
#     records.sort( key = lambda x : x[ -1 ] )
#     
#     timestamps = []
#     for record in records:
#       site = record[ 0 ]
#       samStatus = record[ 1 ]
#       lastUpdateTime = record[ 2 ].strftime( '%Y-%m-%d %H:%M' )
#       if lastUpdateTime not in timestamps:
#         timestamps.append( lastUpdateTime )
#       samHistory[ site ].append( samStatus )
#         
#     ret = S_OK( samHistory )
#     ret[ 'Total' ] = len( timestamps )
#     ret[ 'Timestamps' ] = timestamps
#     
#     return ret
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
