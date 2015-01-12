
from types import DictType, FloatType, IntType, ListType, LongType, StringTypes, TupleType, UnicodeType

import time

# DIRAC
from DIRAC                                import gConfig, gLogger, S_ERROR, S_OK, Time
from DIRAC.Core.DISET.RequestHandler      import RequestHandler, getServiceOption
from DIRAC.Core.DISET.RPCClient           import RPCClient

from BESDIRAC.WorkloadManagementSystem.DB.TaskDB  import TaskDB

__RCSID__ = '$Id: $'

# This is a global instance of the TaskDB class
gTaskDB = None

def initializeTaskManagerHandler( serviceInfo ):

  global gTaskDB

  gTaskDB = TaskDB()

  return S_OK()

class TaskManagerHandler( RequestHandler ):

  def initialize( self ):
    credDict = self.getRemoteCredentials()
    self.rpcProperties   = credDict[ 'properties' ]

    self.__jobMonitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )

  def retrieveTaskProgress( self, jobIDs ):
    result = self.__jobMonitoring.getJobsStatus( jobIDs )
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

    return progress

  def analyseTaskStatus( self, progress ):
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
    result = gTaskDB.updateTaskStatus( taskID, status )
    if not result['OK']:
      return result

    result = gTaskDB.insertTaskHistory( taskID, status, discription )
    if not result['OK']:
      return result

    return S_OK()

  types_getTasks = [ ListType, DictType, [IntType, LongType] ]
  def export_getTasks( self, outFields, condDict, limit = 5 ):
    """ Get task
    """
    return gTaskDB.getTasks( outFields, condDict, limit )

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


  types_refreshTask = [ [IntType, LongType] ]
  def export_refreshTask( self, taskID ):
    """ Refresh a task
    """
    result = gTaskDB.getTaskStatus( taskID )
    if not result['OK']:
      return result
    status = result['Value']

    result = gTaskDB.getTaskJobs( taskID )
    if not result['OK']:
      return result
    jobIDs = result['Value']

    # get task progress from the job list
    progress = self.retrieveTaskProgress( jobIDs )
    gLogger.info( 'Task %d Progress: %s' % ( taskID, progress ) )
    result = gTaskDB.updateTaskProgress( taskID, progress )
    if not result['OK']:
      return result

    # get current task status from the progress
    newStatus = self.analyseTaskStatus( progress )
    gLogger.info( 'Task %d new status: %s' % ( taskID, newStatus ) )
    if newStatus != status:
      result = gTaskDB.updateTaskStatus( taskID, newStatus )
      if not result['OK']:
        return result

    return S_OK( taskID )
