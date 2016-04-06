import os, math
from datetime import datetime, timedelta
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers


__RCSID__ = '$Id: $'

_logPath = '/opt/dirac/pro/DIRAC/ResourceStatusSystem/SAMSystem/log'
_scriptPath = '/opt/dirac/pro/DIRAC/ResourceStatusSystem/SAMSystem/sam_script'


class BaseTest( object ):

    testType = ''
    testGroup = 'SAM-Test'
    elementType = []
    
    def __init__( self, executable, timeout = None):
        self.executable = executable
        self.timeout = timeout or 900
        self.dirac = Dirac()
        self.job = Job()
        self.__initializeJob()
        
    
    def __initializeJob( self ):
        self.job.setName( self.testType )
        self.job.setJobGroup( self.testGroup )
        self.job.setExecutable( self.executable[ 0 ], arguments = self.executable[ 1 ] )
        self.job.setInputSandbox( '%s/%s' % ( _scriptPath, self.executable[ 1 ] ) )
        
    
    def sendTest( self, elementName, elementType ):
        site = None; ce = None
        if elementType is 'ComputingElement':
            ce = elementName
        if elementType is 'CLOUD':
            site = elementName
        
        submissionTime = datetime.utcnow().replace( microsecond = 0 )

        sendRes = self.__submit( site, ce )
        gLogger.debug( 'Submitting %s to %s %s.' % ( self.testType, elementType, elementName ) )
        
        if not sendRes[ 'OK' ]:
            gLogger.debug( 'Submission failed, ', sendRes[ 'Message' ] )
            return sendRes
            
        else:
            jobID = sendRes[ 'Value' ]
            gLogger.debug( 'Submission succeed, job ID %d.' % jobID )
            
        return S_OK( { 'JobID' : jobID, 'SubmissionTime' : submissionTime } )
    
    
    def __submit( self, site = None, CE = None ):
        if site and not CE:
            self.job.setDestination( site )
        if CE:
            self.job.setDestinationCE( CE )
            
        return self.dirac.submit( self.job )
    
    
    def getTestResult( self, jobID, submissionTime ):
        testRes = {
                   'Status' : None,
                   'Log' : None,
                   'CompletionTime' : None,
                   'ApplicationTime' : None
                   }
        isFinish = False
        
        status = self.dirac.status( jobID )
        if not status[ 'OK' ]:
            return status
        status = status[ 'Value' ][ jobID ]
        
        if status[ 'Status' ] in ( 'Done', 'Failed' ):
            isFinish = True
            
            gLogger.debug( 'Downloading log file for %s, job ID %d.' % ( self.testType, jobID ) )
            outputRes = self.dirac.getOutputSandbox( jobID, _logPath )
            
            if not outputRes[ 'OK' ]:
                gLogger.debug( 'Download failed, ', outputRes[ 'Message' ] )
                testRes[ 'Status' ] = 'Warn'
                testRes[ 'Log' ] = 'Fail to download log file for %s: %s' % ( self.testType, outputRes[ 'Message' ] )

            else:
                gLogger.debug( 'Download succeed.' )
                status, log = self.__logResolve( '%s/%d/Script1_python.log' % ( _logPath, jobID ) )
                runtime = self.__getAppRunningTime( log )
                os.system( 'rm -rf %s/%d' % ( _logPath, jobID ) )
                
                testRes[ 'Status' ] = status
                testRes[ 'Log' ] = log
                testRes[ 'ApplicationTime' ] = runtime
            
                #get test completion time
                testRes[ 'CompletionTime' ] = datetime.utcnow().replace( microsecond = 0 )
                    
        else:
            if datetime.utcnow().replace( microsecond = 0 ) - submissionTime >= timedelta( seconds = self.timeout ):
                isFinish = True
                testRes[ 'Status' ] = 'Unknown'
                testRes[ 'Log' ] = 'Test did not complete within the timeout of %d seconds.' % self.timeout
        
        gLogger.debug( 'Test status: %s.' % testRes[ 'Status' ] )
        
        ret =  S_OK( testRes )
        ret[ 'IsFinish' ] = isFinish
        return ret
                
        
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
        #get application running time
        index = log.find( 'Running time:' )
        runtime = ''
        while log[ index ] != '\n':
            runtime += log[ index ]
            index += 1
        runtime = int( math.floor( float( runtime[ len( 'Running time:'  ) : ].strip() ) * 1000 ) )
        
        return runtime
        
        
    @staticmethod
    def _judge( log ):
        return None
        
    

