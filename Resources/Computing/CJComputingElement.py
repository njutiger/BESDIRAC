########################################################################
# File :   CJComputingElement.py
# Author : Zhao Xianghu
########################################################################

""" SSH (Virtual) Computing Element: For a given list of ip/cores pair it will send jobs
    directly through ssh
    It's still under development & debugging,
"""

import os
import socket
import stat
import base64
from urlparse import urlparse

import requests

from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import rootPath

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Resources.Computing.PilotBundle               import bundleProxy, writeScript


CE_NAME = 'CJ'

class CJComputingElement( ComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.submittedJobs = 0

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """

    ##make it executable
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
    
    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if proxy:
      self.log.verbose( 'Setting up proxy for payload' )
      wrapperContent = bundleProxy( executableFile, proxy )
      name = writeScript( wrapperContent, os.getcwd() )
      submitFile = name
    else: # no proxy
      submitFile = executableFile

    # Submit jobs now
    cloudIDs = []
    for i in range(numberOfJobs):
      response = requests.post(self.ceParameters['Url'] + '/jobs', files={ 'file': open(submitFile, 'rb') })
      jobID = response.text
      cloudIDs.append( jobID )
      response = requests.put(self.ceParameters['Url'] + '/jobs/' + jobID, params={'status': 'WAITING'})

    ceHost = self.ceName
    jobIDs = [ 'cloudjob://%s/%s' % ( ceHost, _id ) for _id in cloudIDs ]
        
    self.submittedJobs += len(cloudIDs)

    if proxy:
      os.remove( submitFile )    
            
    return S_OK( jobIDs )        

  def killJob( self, jobIDs ):
    """ Kill specified jobs
    """ 
      
    return S_OK( 0 )

  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0

    response = requests.get(self.ceParameters['Url'] + '/jobs/stat')
    r = response.json()
    if 'RUNNING' in r:
        result['RunningJobs'] = r['RUNNING']
    if 'WAITING' in r:
        result['WaitingJobs'] = r['WAITING']

    self.log.verbose( 'Waiting Jobs: ', result['WaitingJobs'] )
    self.log.verbose( 'Running Jobs: ', result['RunningJobs'] )

    return result

  def getJobStatus ( self, jobIDList ):
    """ Get status of the jobs in the given list
    """
    resultDict = {}
    jobDict = {}
    for job in jobIDList:
      stamp = os.path.basename( urlparse( job ).path )
      jobDict[stamp] = job
    stampList = jobDict.keys()   

    response = requests.post('%s/jobs/status' % self.ceParameters['Url'], json={'job_ids': stampList})
    r = response.json()
    for stamp in r:
      originalStatus = r[stamp]
      if originalStatus in ['INIT']:
        newStatus = 'Submitted'
      elif originalStatus in ['WAITING']:
        newStatus = 'Waiting'
      elif originalStatus in ['RUNNING']:
        newStatus = 'Running'
      elif originalStatus in ['DONE']:
        newStatus = 'Done'
      else:
        newStatus = 'Unknown'
      resultDict[jobDict[stamp]] = newStatus

    for job in jobIDList:
      if not job in resultDict:
        resultDict[job] = 'Unknown'

    return S_OK( resultDict )

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    response = requests.get('%s/jobs/%s/output' % (self.ceParameters['Url'], os.path.basename( urlparse( jobID ).path )))
    r = response.json()
    output = base64.b64decode(r['stdout'])
    error = base64.b64decode(r['stderr'])

    return S_OK( ( output, error ) )  

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
