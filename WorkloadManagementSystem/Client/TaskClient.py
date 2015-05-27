
from DIRAC                                import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient           import RPCClient

class TaskClient( object ):
  """
      The task manager client
  """

  def __init__( self ):
    self.__taskManager = RPCClient('WorkloadManagement/TaskManager')
    self.__jobManager = RPCClient('WorkloadManagement/JobManager')
    self.__jobMonitor = RPCClient('WorkloadManagement/JobMonitoring')


  def listTask( self, condDict, limit, offset, orderAttribute ):
    return self.__taskManager.getTasks( condDict, limit, offset, orderAttribute, 1 )


  def showTask( self, taskID ):
    return self.__taskManager.getTask( taskID, 1 )

  def showTaskJobs( self, taskID ):
    return self.__taskManager.getTaskJobs( taskID )

  def showTaskHistories( self, taskID ):
    return self.__taskManager.getTaskHistories( taskID )


  def showJobs( self, jobIDs, outFields ):
    return self.__taskManager.getJobs( jobIDs, outFields )


  def rescheduleTask( self, taskID, jobStatus=[] ):
    return self.__manageTask( taskID, self.__jobManager.rescheduleJob )


  def deleteTask( self, taskID, jobStatus=[] ):
    result = self.__manageTask( taskID, self.__jobManager.deleteJob )
    if not result['OK']:
      return result
    jobIDs = result['Value']['JobID']

    result = self.__taskManager.removeTask( taskID )
    if not result['OK']:
      return S_ERROR( 'Remove task error: %s' % result['Message'] )

    return S_OK( {'TaskID': taskID, 'JobID': jobIDs} )


  def __manageTask( self, taskID, action, jobStatus=[] ):
    result = self.__taskManager.getTaskJobs( taskID )
    if not result['OK']:
      return S_ERROR( 'Get task jobs error: %s' % result['Message'] )
    jobIDs = result['Value']

    if jobStatus:
      result = self.__jobMonitor.getJobs( { 'JobID': jobIDs, 'Status': jobStatus } )
      if not result['OK']:
        return S_ERROR( 'Get jobs of status %s error: %s' % (status, result['Message']) )
      jobIDs = result['Value']

    result = action( jobIDs )
    if not result['OK']:
      return S_ERROR( 'Manage jobs error (%s): %s' % result['Message'] )

    return S_OK( {'TaskID': taskID, 'JobID': jobIDs} )
