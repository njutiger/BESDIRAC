__RCSID__ = "8f34a5d (2015-01-12 13:07:33 +0000) Xianghu Zhao <zhaoxh@ihep.ac.cn>"

from DIRAC                                            import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                      import AgentModule
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from BESDIRAC.WorkloadManagementSystem.DB.TaskDB      import TaskDB

import time

class TaskAgent( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  def initialize( self ):
    self.__taskManager = RPCClient( 'WorkloadManagement/TaskManager' )
    self.__taskDB = TaskDB()
    return S_OK()

  def execute( self ):
    """ Main execution method
    """
    condDict = { 'Status': ['Ready', 'Processing', 'Finished'] }
    result = self.__taskDB.getTasks( [ 'TaskID', 'Status' ], condDict )
    if not result['OK']:
      return result

    tasks = result['Value']

    self.log.info( '%d tasks will be refreshed' % len(tasks) )

    for task in tasks:
      taskID = task[0]
      status = task[1]

      if status in ['Ready', 'Processing', 'Finished']:
        self.__refreshTask( taskID )

    return S_OK()


  def __refreshTask( self, taskID ):
    result = self.__taskManager.refreshTaskSites( taskID )
    if result['OK']:
      self.log.info( 'Task %d site is refreshed' % taskID )
    else:
      self.log.warn( 'Task %d site refresh failed: %s' % ( taskID, result['Message'] ) )

    result = self.__taskManager.refreshTaskJobGroups( taskID )
    if result['OK']:
      self.log.info( 'Task %d job group is refreshed' % taskID )
    else:
      self.log.warn( 'Task %d job group refresh failed: %s' % ( taskID, result['Message'] ) )

    result = self.__taskManager.refreshTaskStatus( taskID )
    if result['OK']:
      self.log.info( 'Task %d status is refreshed' % taskID )
    else:
      self.log.warn( 'Task %d status refresh failed: %s' % ( taskID, result['Message'] ) )
