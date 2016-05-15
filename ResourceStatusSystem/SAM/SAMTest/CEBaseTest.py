""" CEBaseTest

  Base class for all the CE test classes. 

"""


import os
from datetime                                           import datetime, timedelta
from DIRAC                                              import S_OK, gLogger
from DIRAC.Interfaces.API.Job                           import Job
from DIRAC.Interfaces.API.Dirac                         import Dirac
from BESDIRAC.ResourceStatusSystem.SAM.SAMTest.TestBase import TestBase


__RCSID__ = '$Id: $'


class CEBaseTest( TestBase ):
  """ 
    CEBaseTest is base class for all the CE test classes. Real  CE test should
    implement its _judge method. 
  """
    
  def __init__( self, args = None, apis = None ):
    super( CEBaseTest, self ).__init__( args, apis )
      
    self.executable = self.args[ 'executable' ]
    self.timeout = self.args.get( 'timeout', 1800 )
    self.elementType = ( 'ComputingElement', 'CLOUD' )
    self.testGroup = 'CE-Test'
    self.__logPath = '/opt/dirac/pro/BESDIRAC/ResourceStatusSystem/SAM/log'
    self.__scriptPath = '/opt/dirac/pro/BESDIRAC/ResourceStatusSystem/SAM/sam_script'
    
    if 'Dirac' in self.apis:
      self.dirac = self.apis[ 'Dirac' ]
    else:
      self.dirac = Dirac()    
    

  def doTest( self, elementName, elementType ):
    """
      submit test job to the specified ce or cloud..
    """
    
    super( CEBaseTest, self ).doTest( elementName, elementType )
    
    site = None; ce = None
    if elementType == 'ComputingElement':
      ce = elementName
    if elementType == 'CLOUD':
      site = elementName
        
    submissionTime = datetime.utcnow().replace( microsecond = 0 )

    sendRes = self.__submit( site, ce )
    gLogger.debug( 'Submitting %s to %s %s.' % ( self.testType, elementType, elementName ) )
        
    if not sendRes[ 'OK' ]:
      gLogger.debug( 'Submission failed, ', sendRes[ 'Message' ] )
      return sendRes
    
    jobID = sendRes[ 'Value' ]
    gLogger.debug( 'Submission succeed, job ID %d.' % jobID )
    
    self._updateResult( elementName, JobID = jobID, SubmissionTime = submissionTime )
    
    return S_OK()
    
    
  def __submit( self, site = None, CE = None ):
    """
      set the job and submit.
    """
    
    job = Job()
    job.setName( self.testType )
    job.setJobGroup( self.testGroup )
    job.setExecutable( self.executable[ 0 ], arguments = self.executable[ 1 ] )
    job.setInputSandbox( '%s/%s' % ( self.__scriptPath, self.executable[ 1 ] ) )
    if site and not CE:
      job.setDestination( site )
    if CE:
      job.setDestinationCE( CE )
      
    return self.dirac.submit( job )
    
    
  def getTestResult( self, elementName ):
    """
      download output sandbox and judge the test status from the log file. 
    """
    
    super( CEBaseTest, self ).getTestResult( elementName )
    
    isFinish = False
    jobID = self._testResult[ elementName ][ 'JobID' ]
    submissionTime = self._testResult[ elementName ][ 'SubmissionTime' ]
    
    status = self.dirac.status( jobID )
    if not status[ 'OK' ]:
      return status
    status = status[ 'Value' ][ jobID ]
        
    resDict = { 'CompletionTime' : None, 'Status' : None, 'Log' : None, 'ApplicationTime' : None }
    
    if status[ 'Status' ] in ( 'Done', 'Failed' ):
      isFinish = True
      resDict[ 'CompletionTime' ] = datetime.utcnow().replace( microsecond = 0 )
            
      gLogger.debug( 'Downloading log file for %s, job ID %d.' % ( self.testType, jobID ) )
      outputRes = self.dirac.getOutputSandbox( jobID, self.__logPath )
            
      if not outputRes[ 'OK' ]:
        gLogger.debug( 'Download failed, ', outputRes[ 'Message' ] )
        resDict[ 'Status' ] = 'Warn'
        resDict[ 'Log' ] = 'Fail to download log file for %s: %s' % ( self.testType, outputRes[ 'Message' ] )

      else:
        gLogger.debug( 'Download succeed.' )
        status, log = self.__logResolve( '%s/%d/Script1_python.log' % ( self.__logPath, jobID ) )
        runtime = self.__getAppRunningTime( log )
        os.system( 'rm -rf %s/%d' % ( self.__logPath, jobID ) )
                
        resDict[ 'Status' ] = status
        resDict[ 'Log' ] = log
        resDict[ 'ApplicationTime' ] = runtime
                    
    else:
      if datetime.utcnow().replace( microsecond = 0 ) - submissionTime >= timedelta( seconds = self.timeout ):
        isFinish = True
        resDict[ 'Status' ] = 'Unknown'
        resDict[ 'Log' ] = 'Test did not complete within the timeout of %d seconds.' % self.timeout
        
    self._updateResult( elementName, **resDict )
    
    if not isFinish:
      return S_OK()
    else:
      return S_OK( self._testResult[ elementName ] ) 
                
        
  def __logResolve( self, filepath ):
    jobID = filepath.split( '/' )[ -2 ]
        
    try:
      logfile = open( filepath, 'r' )
      log = logfile.read()
    except IOError, e:
      gLogger.error( "Can't open the log file of job %s, " % jobID, e )
      raise IOError
            
    status = self._judge( log )
    logfile.close()
        
    return ( status, log )
    
    
  @staticmethod
  def __getAppRunningTime( log ):
    index = log.find( 'Running Time :' )
    runtime = ''
    while log[ index ] != '\n':
      runtime += log[ index ]
      index += 1
    runtime = float( runtime[ len( 'Running Time :'  ) : ].strip() )
        
    return runtime
        
        
  @staticmethod
  def _judge( log ):
    """
      to be extended by real ce tests.
    """
      
    return 'OK'