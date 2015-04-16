
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


################################################################################
# exported interfaces

  types_createTask = [ StringTypes, DictType ]
  def export_createTask( self, taskName, taskInfo ):
    """ Create a new task
    """
    status = 'Init'
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

  types_activateTask = [ [IntType, LongType] ]
  def export_activateTask( self, taskID ):
    """ Activate the task
    """
    return gTaskDB.updateTaskActive( taskID, True )

#  types_isTaskActive = [ [IntType, LongType] ]
#  def export_isTaskActive( self, taskID ):
#    """ Check if task is active
#    """
#    result = gTaskDB.getTask( taskID, ['Active'] )
#    if not result['OK']:
#      return result
#    return S_OK( bool( result['Value'][0] ) )

#  types_insertTaskHistory = [ [IntType, LongType], StringTypes, StringTypes ]
#  def export_insertTaskHistory( self, taskID, status, discription = '' ):
#    """ Insert a task history
#    """
#    return gTaskDB.insertTaskHistory( taskID, status, discription )

  types_addTaskJob = [ [IntType, LongType], [IntType, LongType], DictType ]
  def export_addTaskJob( self, taskID, jobID, jobInfo ):
    """ Add a job to the task
    """
    result = gTaskDB.getTask( taskID, ['Active'] )
    if not result['OK']:
      return result

    if result['Value'][0]:
      self.log.error( 'Can not add job to an active task' % taskID )
      return S_ERROR( 'Can not add job to an active task' % taskID )

    return gTaskDB.addTaskJob( taskID, jobID, jobInfo )

  types_updateTaskInfo = [ [IntType, LongType], DictType ]
  def export_updateTaskInfo( self, taskID, taskInfo ):
    """ Update the task info
    """
    result = gTaskDB.getTaskInfo( taskID )
    if not result['OK']:
      return result

    newTaskInfo = result['Value']
    newTaskInfo.update( taskInfo )

    return gTaskDB.updateTaskInfo( taskID, newTaskInfo )

#  types_updateTaskStatus = [ [IntType, LongType], StringTypes, StringTypes ]
#  def export_updateTaskStatus( self, taskID, status, discription ):
#    """ Update status of a task
#    """
#    return self.__updateTaskStatus( taskID, status, discription )

################################################################################

  types_getTasks = [ ListType, DictType, [IntType, LongType], StringTypes ]
  def export_getTasks( self, outFields, condDict, limit, orderAttribute ):
    """ Get task list
    """
    newLimit = limit if limit >= 0 else False
    newOrderAttribute = orderAttribute if orderAttribute else None
    return gTaskDB.getTasks( outFields, condDict, newLimit, newOrderAttribute )

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

  types_getTaskIDFromJob = [ [IntType, LongType] ]
  def export_getTaskIDFromJob( self, jobID ):
    """ Get task ID from job ID
    """
    return gTaskDB.getTaskIDFromJob( jobID )

  types_getJobInfo = [ [IntType, LongType] ]
  def export_getJobInfo( self, jobID ):
    """ Get job info
    """
    return gTaskDB.getJobInfo( jobID )


  types_getTaskJobsStatus = [ [IntType, LongType] ]
  def export_getTaskJobsStatus( self, taskID ):
    """ Get status of all jobs in the task
    """
    return self.__getTaskStatusCount( taskID, 'Status' )

  types_getTaskJobsMinorStatus = [ [IntType, LongType] ]
  def export_getTaskJobsMinorStatus( self, taskID ):
    """ Get minor status of all jobs in the task
    """
    return self.__getTaskStatusCount( taskID, 'MinorStatus' )

  types_getTaskJobsApplicationStatus = [ [IntType, LongType] ]
  def export_getTaskJobsApplicationStatus( self, taskID ):
    """ Get application status of all jobs in the task
    """
    return self.__getTaskStatusCount( taskID, 'ApplicationStatus' )

  types_getTaskJobIDs = [ [IntType, LongType] ]
  def export_getTaskJobIDs( self, taskID, condition ):
    """ Get all job IDs by the condition
    """
    return self

################################################################################

  types_refreshTaskStatus = [ [IntType, LongType] ]
  def export_refreshTaskStatus( self, taskID ):
    """ Refresh a task
    """
    return self.__refreshTaskStatus( taskID )

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


################################################################################
# private functions

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

  def __updateTaskStatus( self, taskID, status, discription ):
    result = gTaskDB.updateTask( taskID, ['Status'], [status] )
    if not result['OK']:
      return result
    result = gTaskDB.insertTaskHistory( taskID, status, discription )
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
    if newStatus == 'Expired':
      gTaskDB.updateTaskActive( taskID, False )
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
