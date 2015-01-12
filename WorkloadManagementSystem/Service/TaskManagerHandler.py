
from types import DictType, FloatType, IntType, ListType, LongType, StringTypes, TupleType, UnicodeType

import time

# DIRAC
from DIRAC                                import gConfig, gLogger, S_ERROR, S_OK, Time
from DIRAC.Core.DISET.RequestHandler      import RequestHandler, getServiceOption

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
    self.rpcProperties   = credDict[ 'properties' ]

  def __getJobAttributesForTask( self, taskID, outFields ):
    result = gTaskDB.getTaskJobs( taskID )
    if not result['OK']:
      return result
    jobIDs = result['Value']

    return gJobDB.getAttributesForJobList( jobIDs, outFields )

  def __getStatusNumber( self, taskID, statusType ):
    result = self.__getJobAttributesForTask( taskID, [statusType] )
    if not result['OK']:
      return result
    statuses = result['Value']

    statusNumber = {}
    for jobID in statuses:
      status = statuses[jobID][statusType]
      if status not in statusNumber:
        statusNumber[status] = 0
      statusNumber[status] += 1

    return S_OK( statusNumber )

  def __retrieveTaskProgress( self, taskID ):
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

  def __analyseTaskStatus( self, progress ):
    totalJob = progress.get( 'Total', 0 )
    runningJob = progress.get( 'Running', 0 )
    waitingJob = progress.get( 'Waiting', 0 )

    status = 'Unknown'
    if totalJob == 0:
      status = 'Empty'
    elif runningJob != 0:
      status = 'Running'
    elif waitingJob == 0:
      status = 'Finished'
    else:
      status = 'Waiting'

    return status

  def __retrieveTaskAttribute( self, taskID, attributeType ):
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

  def __refreshTaskStringAttribute( self, taskID, attributeType ):
    result = gTaskDB.getTask( taskID, [attributeType] )
    if not result['OK']:
      return result
    oldAttributes = result['Value'][0].split( ',' )

    result = self.__retrieveTaskAttribute( taskID, attributeType )
    if not result['OK']:
      return result
    newAttributes = result['Value']

    attributes = list( set( oldAttributes ) | set( newAttributes ) )
    if '' in attributes:
      attributes.remove( '' )

    allAttributes = ','.join( attributes )
    result = gTaskDB.updateTask( taskID, [attributeType], [allAttributes] )
    if not result['OK']:
      return result

    return S_OK( allAttributes )


  types_createTask = [ StringTypes, DictType ]
  def export_createTask( self, taskName, taskInfo ):
    """ Create a new task
    """
    status = 'Created'
    credDict = self.getRemoteCredentials()
    owner = credDict[ 'username' ]
    ownerDN = credDict[ 'DN' ]
    ownerGroup = credDict[ 'group' ]

    result = gTaskDB.createTask( taskName, status, owner, ownerDN, ownerGroup, taskInfo )
    if not result['OK']:
      return result
    taskID = result['Value']

    result = gTaskDB.insertTaskHistory( taskID, status, 'New task created' )
    if not result['OK']:
      return result

    return S_OK( taskID )

  types_insertTaskHistory = [ [IntType, LongType], StringTypes, StringTypes ]
  def export_insertTaskHistory( self, taskID, status, discription = '' ):
    """ Insert a task history
    """
    return gTaskDB.insertTaskHistory( taskID, status, discription )

  types_addTaskJob = [ [IntType, LongType], [IntType, LongType], DictType ]
  def export_addTaskJob( self, taskID, jobID, jobInfo ):
    """ Add a job to the task
    """
    return gTaskDB.addTaskJob( taskID, jobID, jobInfo )

  types_updateTaskStatus = [ [IntType, LongType], StringTypes, StringTypes ]
  def export_updateTaskStatus( self, taskID, status, discription ):
    """ Update status of a task
    """
    result = gTaskDB.updateTask( taskID, ['Status'], [status] )
    if not result['OK']:
      return result
    result = gTaskDB.insertTaskHistory( taskID, status, discription )
    if not result['OK']:
      return result

    return S_OK( status )

  types_getTasks = [ ListType, DictType, [IntType, LongType] ]
  def export_getTasks( self, outFields, condDict, limit = 5, orderAttribute = None ):
    """ Get task list
    """
    return gTaskDB.getTasks( outFields, condDict, limit, orderAttribute )

  types_getTask = [ [IntType, LongType], ListType ]
  def export_getTask( self, taskID, outFields ):
    """ Get task
    """
    return gTaskDB.getTask( taskID, outFields )

  types_getTaskStatus = [ [IntType, LongType] ]
  def export_getTaskStatus( self, taskID ):
    """ Get task status
    """
    return gTaskDB.getTaskStatus( taskID )

  types_getTaskInfo = [ [IntType, LongType] ]
  def export_getTaskInfo( self, taskID ):
    """ Get task info
    """
    return gTaskDB.getTaskInfo( taskID )

  types_getTaskJobs = [ [IntType, LongType] ]
  def export_getTaskJobs( self, taskID ):
    """ Get task jobs
    """
    return gTaskDB.getTaskJobs( taskID )

  types_getJobInfo = [ [IntType, LongType] ]
  def export_getJobInfo( self, jobID ):
    """ Get job info
    """
    return gTaskDB.getJobInfo( jobID )


  types_getJobsStatus = [ [IntType, LongType] ]
  def export_getJobsStatus( self, taskID ):
    """ Get status of all jobs in the task
    """
    return self.__getStatusNumber( taskID, 'Status' )

  types_getJobsMinorStatus = [ [IntType, LongType] ]
  def export_getJobsMinorStatus( self, taskID ):
    """ Get minor status of all jobs in the task
    """
    return self.__getStatusNumber( taskID, 'MinorStatus' )

  types_getJobsApplicationStatus = [ [IntType, LongType] ]
  def export_getJobsApplicationStatus( self, taskID ):
    """ Get application status of all jobs in the task
    """
    return self.__getStatusNumber( taskID, 'ApplicationStatus' )

  types_refreshTask = [ [IntType, LongType] ]
  def export_refreshTask( self, taskID ):
    """ Refresh a task
    """
    # get task progress from the job list
    result = self.__retrieveTaskProgress( taskID )
    if not result['OK']:
      return result
    progress = result['Value']
    gLogger.debug( 'Task %d Progress: %s' % ( taskID, progress ) )
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
    gLogger.debug( 'Task %d new status: %s' % ( taskID, newStatus ) )
    if newStatus != status:
      result = gTaskDB.updateTask( taskID, ['Status'], [newStatus] )
      if not result['OK']:
        return result
      result = gTaskDB.insertTaskHistory( taskID, newStatus, 'Status refreshed' )
      if not result['OK']:
        return result

    return S_OK( newStatus )

  types_refreshTaskSites = [ [IntType, LongType] ]
  def export_refreshTaskSites( self, taskID ):
    """ Refresh sites of a task
    """
    return self.__refreshTaskStringAttribute( taskID, 'Site' )

  types_refreshTaskJobGroup = [ [IntType, LongType] ]
  def export_refreshTaskJobGroup( self, taskID ):
    """ Refresh job groups of a task
    """
    return self.__refreshTaskStringAttribute( taskID, 'JobGroup' )
