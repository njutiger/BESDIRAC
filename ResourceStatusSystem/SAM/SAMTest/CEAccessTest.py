''' CEAccessTest

A test class to test the ssh connection to ces.

'''

import subprocess, re
from datetime import datetime
from DIRAC import S_OK, S_ERROR, gConfig
from BESDIRAC.ResourceStatusSystem.SAM.SAMTest.TestBase import TestBase


class CEAccessTest( TestBase ):
  ''' CEAccessTest
  '''
    
  def __init__(self, args=None, apis=None):
    super( CEAccessTest, self ).__init__( args, apis )
        
    self.timeout = self.args.get( 'timeout', 5 )
        
        
  def doTest(self, elementDict):
    '''
      Use nc tool to test the ssh connection to specified ce.
      Test command is like 'nc -v -w 5 -z 202.122.33.148 22'.
    '''

    elementName = elementDict[ 'ElementName' ]
    elementType = elementDict[ 'ElementType' ]

    host = self.__getConnectParam(elementName, elementType)
    if not host[ 'OK' ]:
      return host
    host, port = host[ 'Value' ]
    
    command = 'nc -v -w %d -z %s %s' % ( self.timeout, host, port )
    submissionTime = datetime.utcnow().replace( microsecond = 0 )
    subp = subprocess.Popen( command, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    stdout, stderr = subp.communicate()
    completionTime = datetime.utcnow().replace( microsecond = 0 )
    applicationTime = (completionTime - submissionTime).total_seconds()
    
    if subp.returncode == 0:
      status = 'OK'
      log = stdout
    else:
      status = 'Bad'
      log = stderr
      
    result = { 'Result' : { 'Status' : status, 
                                           'Log' : log, 
                                           'SubmissionTime' : submissionTime, 
                                           'CompletionTime' : completionTime, 
                                           'ApplicationTime' : applicationTime },
                      'Finish' : True }
    
    return S_OK(result)
  
  
  def __getConnectParam( self, element, elementType ):
    '''
      get the access host and port for the specified ce or cloud.
    '''
    
    if elementType == 'ComputingElement':
      _basePath = 'Resources/Sites'
    
      domains = gConfig.getSections( _basePath )
      if not domains[ 'OK' ]:
        return domains
      domains = domains[ 'Value' ]
    
      for domain in domains:
        sites = gConfig.getSections( '%s/%s' % ( _basePath, domain ) )
        if not sites[ 'OK' ]:
          return sites
        sites = sites[ 'Value' ]
      
        for site in sites:
          ces = gConfig.getValue( '%s/%s/%s/CE' % ( _basePath, domain, site ), '' ).split(',')
          ces = map(lambda str : str.strip(), ces)

          if element in ces:
            host = gConfig.getValue('%s/%s/%s/CEs/%s/SSHHost' % ( _basePath, domain, site, element ))
            if host:
              idx = host.find('/')
              if idx != -1: host = host[ 0 : idx ]
              return S_OK((host, 22))
            else:
              return S_OK((element, 8443))

    if elementType == 'CLOUD':
      _basePath = 'Resources/VirtualMachines/CloudEndpoints'
      _searchKey = ( 'ex_force_auth_url', 'occiURI', 'endpointURL' )
      
      endpoints = gConfig.getSections(_basePath)
      if not endpoints[ 'OK' ]:
        return endpoints
      endpoints = endpoints[ 'Value' ]
      
      for endpoint in endpoints:
        site = gConfig.getValue('%s/%s/siteName' % ( _basePath, endpoint ))
        if site == element:
          url = None
          for key in _searchKey:
            url = gConfig.getValue('%s/%s/%s' % ( _basePath, endpoint, key ))
            if url: break
          return S_OK(re.match(r'https?://(.+):([0-9]+).*', url).groups())
          
    return S_ERROR('%s is not a vaild %s.' % ( element, elementType ))
                  