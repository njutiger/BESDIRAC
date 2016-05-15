""" StatusEvaluator

  StatusEvaluator is used to evaluate elements' SAM status depending on the
  SAM tests results. The SAM status includes resource SAM status and site SAM
  status. Firstly, integrate tests results and get resource SAM status. Then judge
  site SAM status depending on the status of resources which belong to the site.
"""

from DIRAC                                                         import S_OK, S_ERROR, gConfig
from BESDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient


__RCSID__ = '$Id:  $'


class StatusEvaluator(object):
  """ StatusEvaluator
  """
    
  def __init__(self, sites, apis = None):
    """ Constructor
    
    examples:
      >>> sites = { 'CLUSTER' : [ 'CLUSTER.USTC.cn' ],
                              'GRID' : [ 'GRID.JINR.ru' ],
                              'CLOUD' : [ ''CLOUD.IHEP-OPENSTACK.cn' ] }
      >>> evaluator = StatusEvaluator( sites )
      >>> evaluator1 = StatusEvaluator( sites, { 'ResourceManagementClient' : 
                                                                                       ResourceManagementClient } )
    
    :Parameters:
       **sites** - `dict`
         the sites to evaluate SAM status. The sites is grouped by domain.
    """
    
    apis = apis or {}
    self.__sites = []
    self.__intailizeSite( sites )
        
    self.states = { 'OK' : 3, 'Warn' : 2, 'Bad' : 1, 'Unknown' : 0 }
        
    if 'ResourceManagementClient' in apis:
      self.rmClient = apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()
        
     
  def __intailizeSite( self, sites ):
    """
      find ces and ses for all the sites from CS.
    """
    
    for domain, siteNames in sites.items():
      for siteName in siteNames:
        if 'CLOUD' == domain:
          ces = [ siteName ]
        else:
          ces = gConfig.getValue( 'Resources/Sites/%s/%s/CE' % ( domain, siteName ) )
          ces =[ ce.strip() for ce in ces.split( ',' ) ]
        ses = gConfig.getValue( 'Resources/Sites/%s/%s/SE' % ( domain, siteName ) )
        ses =[ se.strip() for se in ses.split( ',' ) ]
        self.__sites.append( { 'SiteName' : siteName, 'SiteType' : domain, 'CE' : ces, 'SE' : ses } )
        
        
  def __statusRule(self, statusList):
    """ The rule to evaluate SAM status. The level of status is defined in self.states.
    The rule calculates average level of input statuses and decides final status with
    the average level.
    
    :Parameters:
      **statusList** - 'list'
        the status list
        
    :return: final status
    """
    
    for status in statusList:
      if status == "":
        statusList.remove(status)
                
    if len(statusList) == 0:
      return ""
        
    statusSum = 0
        
    for status in statusList:
      statusVal = self.states[ status ]
      statusSum = statusSum + statusVal
            
    finalStatusVal = statusSum / len(statusList)
        
    if finalStatusVal >= 2.8:
      finalStatus = 'OK'
    elif 1.2 < finalStatusVal < 2.8:
      finalStatus = 'Warn'
    elif 0 < finalStatusVal <= 1.2:
      finalStatus = 'Bad'
    else:
      finalStatus = 'Unknown'
            
    return finalStatus
 
        
  def __storeSAMStatus(self, resourceStatus, siteStatus):
    """
      store site and resource SAM status.
    """
        
    for resourceStatusDict in resourceStatus:
      resQuery = self.rmClient.addOrModifyResourceSAMStatus(
                                                                 resourceStatusDict[ 'ElementName' ],
                                                                 resourceStatusDict[ 'ElementType' ],
                                                                 resourceStatusDict[ 'Tests' ],
                                                                 resourceStatusDict[ 'Status' ]
                                                                 )
      if not resQuery[ 'OK' ]:
        return resQuery
            
    for siteStatusDict in siteStatus:
      resQuery = self.rmClient.addOrModifySiteSAMStatus(
                                                             siteStatusDict[ 'Site' ],
                                                             siteStatusDict[ 'SiteType' ],
                                                             siteStatusDict[ 'Status' ],
                                                             siteStatusDict[ 'CEStatus' ],
                                                             siteStatusDict[ 'SEStatus' ]
                                                             )
      if not resQuery[ 'OK' ]:
        return resQuery
            
    return S_OK()
            
        
  def evaluate( self, testResults ):
    """ Main method which evaluates site and resource SAM status.
    
    examples:
      >>> records = ( ( 'chenj01.ihep.ac.cn', 'WMS-Test', 'ComputingElement', 'OK', 
                                      'balabala', 1, '2016-5-8 00:00:00', '2016-5-8 00:05:23', 0.1234 ), )
      >>> columns =  ( 'ElementName', 'TestType', 'ElementType', 'Status',
                                       'Log', 'JobID', 'SubmissionTime', 'CompletionTime', 'ApplicationTime' ) 
      >>> evaluator.evaluate( { 'Records' : records, 'Columns' : columns } )
        
    :Parameters:
      **testResults** - `dict`
        the records of tests results.
          
    :return: S_OK / S_ERROR
    """
    
    records = list( testResults[ 'Records' ] )
    columns = testResults[ 'Columns' ]
    if( len( records ) == 0 ):
      return S_ERROR( 'No test results input.' )
      
    nameIndex = columns.index( 'ElementName' )
    typeIndex = columns.index( 'ElementType' )
    testIndex = columns.index( 'TestType' )
    statusIndex = columns.index( 'Status' )
    records.sort( key = lambda x : x[ nameIndex ] )
        
    resourceStatus = {}
    elementName = records[ 0 ][ nameIndex ]
    elementType = records[ 0 ][ typeIndex ]
    tests = []
    status = []
    for record in records:
      if record[ nameIndex ] == elementName:
        tests.append( record[ testIndex ] )
        status.append( record[ statusIndex ] )
      else:
        resourceStatus[ elementName ] = { 'ElementName' : elementName,
                                         'ElementType' : elementType, 
                                         'Tests' : ','.join( tests ), 
                                         'Status' : self.__statusRule( status ) }
        elementName = record[ nameIndex ]
        elementType = record[ typeIndex ]
        tests = [ record[ testIndex ] ]
        status = [ record[ statusIndex ] ]
    resourceStatus[ elementName ] = { 'ElementName' : elementName,
                                     'ElementType' : elementType, 
                                     'Tests' : ','.join( tests ), 
                                     'Status' : self.__statusRule( status ) }
        
    siteStatus = []
    for siteDict in self.__sites:
      ceStatusList = []
      seStatusList = []
      for ce in siteDict[ 'CE' ]:
        if ce in resourceStatus: ceStatusList.append( resourceStatus[ ce ][ 'Status' ] )
      for se in siteDict[ 'SE' ]:
        if se in resourceStatus: seStatusList.append( resourceStatus[ se ][ 'Status' ] )
      ceStatus = self.__statusRule( ceStatusList )
      seStatus = self.__statusRule( seStatusList )
      samStatus = self.__statusRule( [ ceStatus, seStatus ] )
      siteStatus.append( { 'Site' : siteDict[ 'SiteName' ],
                          'SiteType' : siteDict[ 'SiteType' ],
                          'CEStatus' : ceStatus,
                          'SEStatus' : seStatus,
                          'Status' : samStatus } )
            
    storeRes = self.__storeSAMStatus( resourceStatus.values(), siteStatus )
    if not storeRes[ 'OK' ]:
      return storeRes
        
    return S_OK()