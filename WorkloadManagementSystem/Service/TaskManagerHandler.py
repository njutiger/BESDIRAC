
from types import DictType, FloatType, IntType, ListType, LongType, StringTypes, TupleType, UnicodeType

import time
import json

# DIRAC
from DIRAC                                import gConfig, gLogger, S_ERROR, S_OK, Time
from DIRAC.Core.DISET.RequestHandler      import RequestHandler, getServiceOption
from DIRAC.Core.DISET.RPCClient           import RPCClient
from DIRAC.Core.Security                  import Properties

from DIRAC.WorkloadManagementSystem.DB.JobDB  import JobDB
from BESDIRAC.WorkloadManagementSystem.DB.TaskDB  import TaskDB

__RCSID__ = '$Id: $'

# This is a global instance of the TaskDB class
gTaskDB = None
gJobDB = None

def initializeTaskManagerHandler( serviceInfo ):

  global gTaskDB, gJobDB

  gTaskDB = TaskDB()
  gJobDB = JobDB()

  return S_OK()

class TaskManagerHandler( RequestHandler ):

  def initialize( self ):
    credDict = self.getRemoteCredentials()
    self.owner          = credDict[ 'username' ]
    self.ownerDN        = credDict[ 'DN' ]
    self.ownerGroup     = credDict[ 'group' ]
    self.userProperties = credDict[ 'properties' ]


################################################################################
# exported interfaces

  types_createTask = [ StringTypes, DictType ]
  def export_createTask( self, taskName, taskInfo ):
    """ Create a new task
    """
    if Properties.NORMAL_USER not in self.userProperties and Properties.JOB_ADMINISTRATOR not in self.userProperties:
      return S_ERROR( 'Access denied to create task' )

    status = 'Init'

    result = gTaskDB.createTask( taskName, status, self.owner, self.ownerDN, self.ownerGroup, taskInfo )
    if not result['OK']:
      return result
    taskID = result['Value']

    result = gTaskDB.insertTaskHistory( taskID, status, 'New task created' )
    if not result['OK']:
      return result

    return S_OK( taskID )

  types_activateTask = [ [IntType, LongType] ]
  def export_activateTask( self, taskID ):
    """ Activate the task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to activate task %s' % taskID )

    return self.__updateTaskStatus( taskID, 'Ready', 'Task is activated' )

  types_removeTask = [ [IntType, LongType] ]
  def export_removeTask( self, taskID ):
    """ Delete the task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to remove task %s' % taskID )

    return self.__updateTaskStatus( taskID, 'Removed', 'Task is removed' )

  types_addTaskJob = [ [IntType, LongType], [IntType, LongType], DictType ]
  def export_addTaskJob( self, taskID, jobID, jobInfo ):
    """ Add a job to the task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied for task %s: Can not add job %s to task' % ( taskID, jobID ) )
    if not self.__hasJobAccess( jobID ):
      return S_ERROR( 'Access denied for job %s: Can not add job to task %s' % ( jobID, taskID ) )

    result = gTaskDB.getTaskStatus( taskID )
    if not result['OK']:
      return result
    status = result['Value']

    if status != 'Init':
      self.log.error( 'Can only add job to "Init" status: task %s' % taskID )
      return S_ERROR( 'Can only add job to "Init" status: task %s' % taskID )

    return gTaskDB.addTaskJob( taskID, jobID, jobInfo )

  types_updateTaskInfo = [ [IntType, LongType], DictType ]
  def export_updateTaskInfo( self, taskID, taskInfo ):
    """ Update the task info
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to update task info for task %s' % taskID )

    result = gTaskDB.getTaskInfo( taskID )
    if not result['OK']:
      return result

    newTaskInfo = result['Value']
    newTaskInfo.update( taskInfo )

    return gTaskDB.updateTaskInfo( taskID, newTaskInfo )

################################################################################

  types_getTasks = [ DictType, [IntType, LongType], [IntType, LongType], StringTypes, [IntType, LongType] ]
  def export_getTasks( self, condDict, limit, offset, orderAttribute, realTimeProgress ):
    """ Get task list
    """
    if Properties.NORMAL_USER not in self.userProperties and Properties.JOB_ADMINISTRATOR not in self.userProperties:
      return S_ERROR( 'Access denied to get tasks' )

    if Properties.NORMAL_USER in self.userProperties:
      condDict['OwnerDN'] = self.ownerDN
      condDict['OwnerGroup'] = self.ownerGroup

      if 'Status' not in condDict:
        condDict['Status'] = [ 'Init', 'Ready', 'Processing', 'Finished', 'Expired' ]
      else:
        condDict['Status'] = list( condDict['Status'] )
        condDict['Status'] = [ v for v in condDict['Status'] if v != 'Removed' ]

    if limit < 0:
      limit = False
    outFields = ['TaskID', 'TaskName', 'Status', 'Owner', 'OwnerDN', 'OwnerGroup', 'CreationTime', 'UpdateTime', 'JobGroup', 'Site']
    if not realTimeProgress:
      outFields.append( 'Progress' )
    result = gTaskDB.getTasks( outFields, condDict, limit, offset, orderAttribute )
    if not result['OK']:
      self.log.error( result['Message'] )
      return S_ERROR( result['Message'] )

    tasks = []
    for outValues in result['Value']:
      progress = {}
      if realTimeProgress:
        result = self.__getTaskProgress( outValues[0] )
        if not result['OK']:
          self.log.error( result['Message'] )
          return result
        progress = result['Value']
      tasks.append( self.__generateTaskResult( outFields, outValues, progress ) )

    return S_OK( tasks )

  types_getTask = [ [IntType, LongType], [IntType, LongType] ]
  def export_getTask( self, taskID, realTimeProgress ):
    """ Get task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task for task %s' % taskID )

    outFields = ['TaskID', 'TaskName', 'Status', 'Owner', 'OwnerDN', 'OwnerGroup', 'CreationTime', 'UpdateTime', 'JobGroup', 'Site', 'Info']
    if not realTimeProgress:
      outFields.append( 'Progress' )
    result = gTaskDB.getTask( taskID, outFields )
    if not result['OK']:
      self.log.error( result['Message'] )
      return S_ERROR( result['Message'] )
    outValues = result['Value']

    progress = {}
    if realTimeProgress:
      result = self.__getTaskProgress( taskID )
      if not result['OK']:
        self.log.error( result['Message'] )
        return result
      progress = result['Value']
    return S_OK( self.__generateTaskResult( outFields, outValues, progress ) )

  types_getTaskStatus = [ [IntType, LongType] ]
  def export_getTaskStatus( self, taskID ):
    """ Get task status
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task status for task %s' % taskID )

    return gTaskDB.getTaskStatus( taskID )

  types_getTaskInfo = [ [IntType, LongType] ]
  def export_getTaskInfo( self, taskID ):
    """ Get task info
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task info for task %s' % taskID )

    return gTaskDB.getTaskInfo( taskID )

  types_getTaskHistories = [ [IntType, LongType] ]
  def export_getTaskHistories( self, taskID ):
    """ Get task histories
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task histories for task %s' % taskID )

    return gTaskDB.getTaskHistories( taskID )

  types_getTaskJobs = [ [IntType, LongType] ]
  def export_getTaskJobs( self, taskID ):
    """ Get task jobs
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task jobs for task %s' % taskID )

    return gTaskDB.getTaskJobs( taskID )

  types_getJobs = [ ListType, ListType ]
  def export_getJobs( self, jobIDs, outFields ):
    """ Get jobs
    """
    jobsIDs = self.__filterJobAccess( jobIDs, outFields ):

    return gTaskDB.getJobs( jobIDs )

  types_getTaskIDFromJob = [ [IntType, LongType] ]
  def export_getTaskIDFromJob( self, jobID ):
    """ Get task ID from job ID
    """
    if not self.__hasJobAccess( jobID ):
      return S_ERROR( 'Access denied to get task ID from job %s' % jobID )

    return gTaskDB.getTaskIDFromJob( jobID )

  types_getJobInfo = [ [IntType, LongType] ]
  def export_getJobInfo( self, jobID ):
    """ Get job info
    """
    if not self.__hasJobAccess( jobID ):
      return S_ERROR( 'Access denied to get job info for job %s' % jobID )

    return gTaskDB.getJobInfo( jobID )


  types_getTaskJobsStatus = [ [IntType, LongType] ]
  def export_getTaskJobsStatus( self, taskID ):
    """ Get status of all jobs in the task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task jobs status for task %s' % taskID )

    return self.__getTaskStatusCount( taskID, 'Status' )

  types_getTaskJobsMinorStatus = [ [IntType, LongType] ]
  def export_getTaskJobsMinorStatus( self, taskID ):
    """ Get minor status of all jobs in the task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task jobs minor status for task %s' % taskID )

    return self.__getTaskStatusCount( taskID, 'MinorStatus' )

  types_getTaskJobsApplicationStatus = [ [IntType, LongType] ]
  def export_getTaskJobsApplicationStatus( self, taskID ):
    """ Get application status of all jobs in the task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task jobs application status for task %s' % taskID )

    return self.__getTaskStatusCount( taskID, 'ApplicationStatus' )

  types_getTaskJobIDs = [ [IntType, LongType] ]
  def export_getTaskJobIDs( self, taskID, condition ):
    """ Get all job IDs by the condition
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to get task job IDs for task %s' % taskID )

    return self


################################################################################

  types_refreshTaskStatus = [ [IntType, LongType] ]
  def export_refreshTaskStatus( self, taskID ):
    """ Refresh a task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to refresh task status for task %s' % taskID )

    return self.__refreshTaskStatus( taskID )

  types_refreshTaskSites = [ [IntType, LongType] ]
  def export_refreshTaskSites( self, taskID ):
    """ Refresh sites of a task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to refresh task sites for task %s' % taskID )

    return self.__refreshTaskStringAttribute( taskID, 'Site' )

  types_refreshTaskJobGroups = [ [IntType, LongType] ]
  def export_refreshTaskJobGroups( self, taskID ):
    """ Refresh job groups of a task
    """
    if not self.__hasTaskAccess( taskID ):
      return S_ERROR( 'Access denied to refresh task job groups for task %s' % taskID )

    return self.__refreshTaskStringAttribute( taskID, 'JobGroup' )


################################################################################
# private functions

  def __isSameDNGroup( self, taskID ):
    result = gTaskDB.getTask( taskID, ['OwnerDN', 'OwnerGroup'] )
    if not result['OK']:
      self.log.error( result['Message'] )
      return False
    if not result['Value']:
      self.log.error( 'Task ID %s not found' % taskID )
      return False

    taskOwnerDN, taskOwnerGroup = result['Value']
    if self.ownerDN == taskOwnerDN and self.ownerGroup == taskOwnerGroup:
      return True

    return False

  def __hasTaskAccess( self, taskID ):
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      return True

    if Properties.NORMAL_USER in self.userProperties:
      if self.__isSameDNGroup( taskID ):
        result = gTaskDB.getTaskStatus( taskID )
        if result['OK'] and result['Value'] != 'Removed':
          return True

    return False

  def __isSameDNGroupForJob( self, jobID ):
    result = gJobDB.getAttributesForJobList( [jobID], ['OwnerDN', 'OwnerGroup'] )
    if not result['OK']:
      self.log.error( result['Message'] )
      return False
    if not result['Value']:
      self.log.error( 'Job %s not found' % jobID )
      return False
    jobOwnerDN = result['Value'][jobID]['OwnerDN']
    jobOwnerGroup = result['Value'][jobID]['OwnerGroup']

    if self.ownerDN == jobOwnerDN and self.ownerGroup == jobOwnerGroup:
      return True

    return False

  def __hasJobAccess( self, jobID ):
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      return True

    if Properties.NORMAL_USER in self.userProperties:
      if self.__isSameDNGroupForJob( jobID ):
        return True

    return False

  def __filterSameDNGroupForJob( self, jobIDs ):
    result = gJobDB.getAttributesForJobList( jobIDs, ['OwnerDN', 'OwnerGroup'] )
    if not result['OK']:
      self.log.error( result['Message'] )
      return []
    if not result['Value']:
      self.log.error( 'Job %s not found' % jobIDs )
      return []

    return [ jobID for jobID in result['Value'].keys() if self.ownerDN == result['Value'][jobID]['OwnerDN'] and self.ownerGroup == result['Value'][jobID]['OwnerGroup'] ]

  def __filterJobAccess( self, jobIDs ):
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      return jobIDs

    if Properties.NORMAL_USER in self.userProperties:
      return self.__filterSameDNGroupForJob( jobIDs )

    return []


  def __generateTaskResult( self, outFields, outValues, progress ):
    taskResult = {}
    for k,v in zip(outFields, outValues):
      if k in ['Progress', 'Info']:
        taskResult[k] = json.loads( v )
      else:
        taskResult[k] = v

    if progress:
      taskResult['Progress'] = progress

    return taskResult


  def __getTaskJobAttributes( self, taskID, outFields ):
    result = gTaskDB.getTaskJobs( taskID )
    if not result['OK']:
      return result
    jobIDs = result['Value']

    return gJobDB.getAttributesForJobList( jobIDs, outFields )

  def __getTaskStatusCount( self, taskID, statusType ):
    result = self.__getTaskJobAttributes( taskID, [statusType] )
    if not result['OK']:
      return result
    statuses = result['Value']

    statusCount = {}
    for jobID in statuses:
      status = statuses[jobID][statusType]
      if status not in statusCount:
        statusCount[status] = 0
      statusCount[status] += 1

    return S_OK( statusCount )

  def __getTaskProgress( self, taskID ):
    result = gTaskDB.getTaskJobs( taskID )
    if not result['OK']:
      return result
    jobIDs = result['Value']

    result = gJobDB.getAttributesForJobList( jobIDs, ['Status'] )
    if not result['OK']:
      return result
    statuses = result['Value']

    progress = { 'Total': 0, 'Done': 0, 'Failed': 0, 'Running': 0, 'Waiting': 0, 'Deleted': 0 }
    progress['Total'] = len(jobIDs)
    for jobID in jobIDs:
      if jobID in statuses:
        status = statuses[jobID]['Status']
        if status in ['Done']:
          progress['Done'] += 1
        elif status in ['Failed', 'Stalled', 'Killed']:
          progress['Failed'] += 1
        elif status in ['Running', 'Completed']:
          progress['Running'] += 1
        else:
          progress['Waiting'] += 1
      else:
        progress['Deleted'] += 1

    return S_OK( progress )

  def __getTaskAttribute( self, taskID, attributeType ):
    """ Get all attributes of the jobs in the task
    """
    result = gTaskDB.getTaskJobs( taskID )
    if not result['OK']:
      return result
    jobIDs = result['Value']

    condDict = { 'JobID': jobIDs }

    result = gJobDB.getDistinctJobAttributes( attributeType, condDict )
    if not result['OK']:
      return result
    attributes = result['Value']

    return S_OK( attributes )

  def __updateTaskStatus( self, taskID, status, description ):
    result = gTaskDB.updateTask( taskID, ['Status'], [status] )
    if not result['OK']:
      return result
    result = gTaskDB.insertTaskHistory( taskID, status, description )
    if not result['OK']:
      return result

    return S_OK( status )

  def __analyseTaskStatus( self, progress ):
    totalJob = progress.get( 'Total', 0 )
    runningJob = progress.get( 'Running', 0 )
    waitingJob = progress.get( 'Waiting', 0 )
    deletedJob = progress.get( 'Deleted', 0 )

    status = 'Unknown'
    if totalJob == 0:
      status = 'Init'
    elif deletedJob == totalJob:
      status = 'Expired'
    elif runningJob == 0 and waitingJob == 0:
      status = 'Finished'
    else:
      status = 'Processing'

    return status


  def __refreshTaskStatus( self, taskID ):
    """ Refresh the task status
    """
    # get task progress from the job list
    result = self.__getTaskProgress( taskID )
    if not result['OK']:
      return result
    progress = result['Value']
    self.log.debug( 'Task %d Progress: %s' % ( taskID, progress ) )
    result = gTaskDB.updateTaskProgress( taskID, progress )
    if not result['OK']:
      return result

    # get previous task status
    result = gTaskDB.getTaskStatus( taskID )
    if not result['OK']:
      return result
    status = result['Value']

    # get current task status from the progress
    newStatus = self.__analyseTaskStatus( progress )
    self.log.debug( 'Task %d new status: %s' % ( taskID, newStatus ) )
    if newStatus != status:
      self.__updateTaskStatus( taskID, newStatus, 'Status refreshed' )
      if not result['OK']:
        return result

    return S_OK( newStatus )

  def __refreshTaskStringAttribute( self, taskID, attributeType ):
    """ Refresh the task attribute. The attribute type must be string and seperated by comma
    """
    # get task attibutes from the job list
    result = self.__getTaskAttribute( taskID, attributeType )
    if not result['OK']:
      return result
    newAttributes = result['Value']

    # get previous task attributes
    result = gTaskDB.getTask( taskID, [attributeType] )
    if not result['OK']:
      return result
    oldAttributes = result['Value'][0].split( ',' )

    # make a combination of old and new attributes
    attributes = list( set( oldAttributes ) | set( newAttributes ) )
    for emptyAttr in [ '', 'ANY', 'Multiple' ]:
      if emptyAttr in attributes:
        attributes.remove( emptyAttr )

    # generate a new attribute
    allAttributes = ','.join( attributes )
    result = gTaskDB.updateTask( taskID, [attributeType], [allAttributes] )
    if not result['OK']:
      return result

    return S_OK( allAttributes )
