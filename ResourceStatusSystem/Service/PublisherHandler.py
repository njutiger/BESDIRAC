# $HeadURL $
""" PublisherHandler

This service has been built to provide the RSS web views with all the information
they need. NO OTHER COMPONENT THAN Web controllers should make use of it.

"""

from datetime import datetime
from types    import NoneType

# DIRAC
from DIRAC                                                      import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.RequestHandler                            import RequestHandler
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers, Utils
ResourceManagementClient = getattr(Utils.voimport( 'DIRAC.ResourceStatusSystem.Client.ResourceManagementClient' ),'ResourceManagementClient')

__RCSID__ = '$Id: PublisherHandler.py 65921 2013-05-14 13:05:43Z ubeda $'

# RSS Clients
rsClient  = None
rmClient  = None

def initializePublisherHandler( _serviceInfo ):
  """
  Handler initialization in the usual horrible way.
  """
  
  global rsClient 
  rsClient = ResourceStatusClient()
  
  global rmClient 
  rmClient = ResourceManagementClient()
  
  return S_OK()
  
class PublisherHandler( RequestHandler ):
  """
  RPCServer used to deliver data to the web portal.
      
  """  

  def __init__( self, *args, **kwargs ):
    """
    Constructor
    """
    super( PublisherHandler, self ).__init__( *args, **kwargs )
  
  # ResourceStatusClient .......................................................
  
  types_getSites = []
  def export_getSites( self ):  
    """
    Returns list of all sites considered by RSS
    
    :return: S_OK( [ sites ] ) | S_ERROR
    """
    
    gLogger.info( 'getSites' )
    return CSHelpers.getSites()

  types_getSitesResources = [ ( str, list, NoneType ) ]
  def export_getSitesResources( self, siteNames ):
    """
    Returns dictionary with SEs and CEs for the given site(s). If siteNames is
    None, all sites are taken into account.
    
    :return: S_OK( { site1 : { ces : [ ces ], 'ses' : [ ses  ] },... } ) | S_ERROR 
    """
    
    gLogger.info( 'getSitesResources' )
        
    if siteNames is None:
      siteNames = CSHelpers.getSites()
      if not siteNames[ 'OK' ]:
        return siteNames
      siteNames = siteNames[ 'Value' ]
    
    if isinstance( siteNames, str ):
      siteNames = [ siteNames ]
    
    sitesRes = {}
    
    for siteName in siteNames:
      
      res = {}      
      res[ 'ces' ] = CSHelpers.getSiteComputingElements( siteName )
      # Convert StorageElements to host names
      ses          = CSHelpers.getSiteStorageElements( siteName )
      sesHosts     = CSHelpers.getStorageElementsHosts( ses )
      if not sesHosts[ 'OK' ]:
        return sesHosts
      # Remove duplicates
      res[ 'ses' ] = list( set( sesHosts[ 'Value' ] ) )
          
      sitesRes[ siteName ] = res
    
    return S_OK( sitesRes )

  types_getElementStatuses = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                               ( str, list, NoneType ), ( str, list, NoneType ),
                               ( str, list, NoneType ) ]
  def export_getElementStatuses( self, element, name, elementType, statusType, status, tokenOwner ):
    """
    Returns element statuses from the ResourceStatusDB
    """
    
    gLogger.info( 'getElementStatuses' )
    return rsClient.selectStatusElement( element, 'Status', name = name, elementType = elementType, 
                                         statusType = statusType, status = status, 
                                         tokenOwner = tokenOwner ) 

  types_getElementHistory = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                              ( str, list, NoneType ) ]
  def export_getElementHistory( self, element, name, elementType, statusType ):
    """
    Returns element history from ResourceStatusDB
    """
    
    gLogger.info( 'getElementHistory' )
    columns = [ 'Status', 'DateEffective', 'Reason' ]
    return rsClient.selectStatusElement( element, 'History', name = name, elementType = elementType,
                                         statusType = statusType,
                                         meta = { 'columns' : columns } ) 

  types_getElementPolicies = [ str, ( str, list, NoneType ), ( str, list, NoneType ) ]
  def export_getElementPolicies( self, element, name, statusType ):
    """
    Returns policies for a given element
    """
    
    gLogger.info( 'getElementPolicies' )
    columns = [ 'Status', 'PolicyName', 'DateEffective', 'LastCheckTime', 'Reason' ]
    return rmClient.selectPolicyResult( element = element, name = name, 
                                        statusType = statusType, 
                                        meta = { 'columns' : columns } )

  types_getTree = [ str, str, str ]
  def export_getTree( self, element, elementType, elementName ):
    """
    Given an element, finds its parent site and returns all descendants of that
    site.
    """

    gLogger.info( 'getTree' )

    site = self.getSite( element, elementType, elementName )        
    if not site:
      return S_ERROR( 'No site' )
    
    siteStatus = rsClient.selectStatusElement( 'Site', 'Status', name = site, 
                                               meta = { 'columns' : [ 'StatusType', 'Status' ] } )
    if not siteStatus[ 'OK' ]:
      return siteStatus      

    tree = { site : { 'statusTypes' : dict( siteStatus[ 'Value' ] ) } }
    
    ces = CSHelpers.getSiteComputingElements( site )    
    cesStatus = rsClient.selectStatusElement( 'Resource', 'Status', name = ces,
                                              meta = { 'columns' : [ 'Name', 'StatusType', 'Status'] } )
    if not cesStatus[ 'OK' ]:
      return cesStatus
    
    ses = CSHelpers.getSiteStorageElements( site )
    sesStatus = rsClient.selectStatusElement( 'Resource', 'Status', name = ses,
                                              meta = { 'columns' : [ 'Name', 'StatusType', 'Status'] } )
    if not sesStatus[ 'OK' ]:
      return sesStatus   
    
    def feedTree( elementsList ):
      
      elements = {}
      for elementTuple in elementsList[ 'Value' ]:
        name, statusType, status = elementTuple
        
        if not name in elements:
          elements[ name ] = {}
        elements[ name ][ statusType ] = status
      
      return elements
    
    tree[ site ][ 'ces' ] = feedTree( cesStatus )
    tree[ site ][ 'ses' ] = feedTree( sesStatus )
    
    return S_OK( tree )
    
  #-----------------------------------------------------------------------------  
    
  def getSite( self, element, elementType, elementName ):
    """
    Given an element, return its site
    """
    
    if elementType == 'StorageElement':
      elementType = 'SE'

    domainNames = gConfig.getSections( 'Resources/Sites' )
    if not domainNames[ 'OK' ]:
      return domainNames
    domainNames = domainNames[ 'Value' ]
  
    for domainName in domainNames:
      
      sites = gConfig.getSections( 'Resources/Sites/%s' % domainName )
      if not sites[ 'OK' ]:
        continue
      
      for site in sites[ 'Value' ]:
      
        elements = gConfig.getValue( 'Resources/Sites/%s/%s/%s' % ( domainName, site, elementType ), '' )
        if elementName in elements:
          return site          

    return ''

  # ResourceManagementClient ...................................................
  
  types_getDowntimes = [ str, str, str ]
  def export_getDowntimes( self, element, elementType, name ):
    
    if elementType == 'StorageElement':
      name = CSHelpers.getSEHost( name )
      if not name['OK']:
        return name
      name = name['Value']
    
    return rmClient.selectDowntimeCache( element = element, name = name, 
                                         meta = { 'columns' : [ 'StartDate', 'EndDate', 
                                                                'Link', 'Description', 
                                                                'Severity' ] } )

  types_getCachedDowntimes = [ ( str, NoneType, list ), ( str, NoneType, list ), ( str, NoneType, list ),
                               ( str, NoneType, list ), datetime, datetime ]
  def export_getCachedDowntimes( self, element, elementType, name, severity, startDate, endDate ):
    
    if elementType == 'StorageElement':
      name = CSHelpers.getSEHost( name )
      if not name['OK']:
        return name
      name = name['Value']
   
    if startDate > endDate:
      return S_ERROR( 'startDate > endDate' )
    
    res = rmClient.selectDowntimeCache( element = element, name = name, severity = severity,
                                        meta = { 'columns' : [ 'Element', 'Name', 'StartDate',
                                                               'EndDate', 'Severity',
                                                               'Description', 'Link' ] } )
    if not res[ 'OK' ]:
      return res
    
    downtimes = []
    
    for dt in res[ 'Value' ]:
      
      dtDict = dict( zip( res[ 'Columns' ], dt ) ) 
    
      if dtDict[ 'StartDate' ] < endDate and dtDict[ 'EndDate' ] > startDate:
        downtimes.append( dt )
    
    result = S_OK( downtimes )
    result[ 'Columns' ] = res[ 'Columns' ]
    
    return result    


  # My Methods ...................................................
  
  types_getVOs = []
  def export_getVOs( self ):
      return gConfig.getSections( 'Registry/VO' )
  
  types_getDomains = []
  def export_getDomains( self ):
    return gConfig.getSections( 'Resources/Sites' )
  
  types_getSiteVO = [ str ]
  def export_getSiteVO( self, siteName ):
      return CSHelpers.getSiteVO( siteName )
  
  types_getSiteResource = [ str ]
  def export_getSiteResource( self, siteName ):
      siteType = siteName.split( '.' )[ 0 ]
      
      if siteType == 'CLOUD':
          ces = []
      else:
          ces = CSHelpers.getSiteComputingElements(siteName)
          
      ses = CSHelpers.getSiteStorageElements( siteName )
      
      return S_OK( { 'ComputingElement' : ces, 'StorageElement' : ses } )
  
  
  types_getSiteSummary = [ ( str, NoneType, list ) ]
  def export_getSiteSummary( self, siteNames ):
      samSummary = {}
      for siteName in siteNames:
          samSummary[ siteName ] = {}
      
      queryRes = rmClient.selectSiteSAMStatusCache( site = siteNames )
      if not queryRes[ 'OK' ]:
          return queryRes
      records = queryRes[ 'Value' ]
      columns = queryRes[ 'Columns' ]
      
      siteSAMStatus = {}
      
      for record in records:
        samDict = dict( zip( columns, record ) )
        siteName = samDict[ 'Site' ]
        siteSAMStatus[ siteName ] = {}
        siteSAMStatus[ siteName ][ 'SAMStatus' ] = samDict[ 'SAMStatus' ] or '-'
        siteSAMStatus[ siteName ][ 'CEStatus' ] = samDict[ 'CEStatus' ] or '-'
        siteSAMStatus[ siteName ][ 'SEStatus' ] = samDict[ 'SEStatus' ] or '-'
        
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
              siteSummary[ siteName ].update( { 'CEStatus' : '-', 'SEStatus' : '-', 'SAMStatus' : '-' } )
              
          if siteJob.has_key( siteName ):
              siteSummary[ siteName ].update( siteJob[ siteName ] )
          else:
              siteSummary[ siteName ].update( { 'Running' : 0, 'Waiting' : 0, 'Done' : 0, 'Failed' : 0, 'Completed' : 0, 'Stalled' : 0, 'Efficiency' : 0.0 } )
        
      return S_OK( siteSummary )
  
  types_getHostEfficiency = [ str ]
  def export_getHostEfficiency( self, siteName ):
      from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
      
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
     
  
  types_getSiteSAMDetailWeb = [ str, ( str, NoneType, list ) ]
  def export_getSiteSAMDetailWeb( self, siteName, columnNames ):
      samDetail = []
      testElements = {}
      
      siteType = siteName.split( '.' )[ 0 ]
      if siteType == 'CLOUD':
          testElements[ siteName ] = 'CLOUD'
      else:
          ces = CSHelpers.getSiteComputingElements( siteName )
          for ce in ces:
              testElements[ ce ] = 'ComputingElement'
              
      ses = CSHelpers.getSiteStorageElements( siteName )
      for se in ses:
          testElements[ se ] = 'StorageElement'
      
      queryRes = rmClient.selectResourceSAMStatusCache( elementName = testElements.keys() )
      if not queryRes[ 'OK' ]:
          return queryRes
      records = queryRes[ 'Value' ]
      columns = queryRes[ 'Columns' ]
      
      if columnNames is None:
          columnNames = columns
      
      for record in records:
          resSAMSummary = {}
          
          for i in range( len( columns ) ):
              columnName = columns[ i ]
              if columnName == 'ElementName':
                  elementName = record[ i ]
                  testElements.pop( elementName )
              if columnName in columnNames:
                  if columnName == 'LastUpdateTime':
                      resSAMSummary[ columnName ] = record[ i ].strftime( '%Y-%m-%d %H:%M:%S' )
                  else:
                      resSAMSummary[ columnName ] = record[ i ]
          
          samDetail.append( resSAMSummary )
          
      for elementName, elementType in testElements.items():
          resSAMSummary = {}
          
          for columnName in columnNames:
              if columnName == 'ElementType':
                  resSAMSummary[ columnName ] = elementType
              elif columnName == 'ElementName':
                  resSAMSummary[ columnName ] = elementName
              else:
                  resSAMSummary[ columnName ] = None
                  
          samDetail.append( resSAMSummary )
          
      return S_OK( samDetail )
  
  
  types_getSitesSAMHistoryWeb = [ ( str, NoneType, list ), datetime, datetime ]
  def export_getSitesSAMHistoryWeb( self, siteNames, fromDate, toDate ):
    if fromDate > toDate:
        return S_ERROR( 'fromDate > toDate.' )
    
    samHistory = {}
    for siteName in siteNames:
        samHistory[ siteName ] = []
    
    fromDate = datetime.strftime( fromDate, '%Y-%m-%d %H:%M:%S' )
    toDate = datetime.strftime( toDate, '%Y-%m-%d %H:%M:%S' )
    queryRes = rmClient.selectSiteSAMStatusHistory(
                                                   site = siteNames,
                                                   meta = { 'newer' : ['LastUpdateTime', fromDate ],
                                                                    'older' : [ 'LastUpdateTime', toDate ],
                                                                    'columns' : [ 'Site', 'SAMStatus', 'LastUpdateTime' ] }
                                                   )
    if not queryRes[ 'OK' ]:
        return queryRes
    records = list( queryRes[ 'Value' ] )
    records.sort( key = lambda x : x[ -1 ] )
    
    timestamps = []
    for record in records:
        site = record[ 0 ]
        samStatus = record[ 1 ]
        lastUpdateTime = record[ 2 ].strftime( '%Y-%m-%d %H:%M' )
        if lastUpdateTime not in timestamps:
            timestamps.append( lastUpdateTime )
        samHistory[ site ].append( samStatus )
        
    ret = S_OK( samHistory )
    ret[ 'Total' ] = len( timestamps )
    ret[ 'Timestamps' ] = timestamps
    
    return ret
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
