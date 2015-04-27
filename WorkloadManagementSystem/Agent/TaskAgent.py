__RCSID__ = "8f34a5d (2015-01-12 13:07:33 +0000) Xianghu Zhao <zhaoxh@ihep.ac.cn>"

from DIRAC                                import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.Core.DISET.RPCClient           import RPCClient

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
    return S_OK()

  def execute( self ):
    """ Main execution method
    """
    condDict = { 'Status': ['Ready', 'Processing', 'Finished', 'Rescheduling', 'Deleting'] }
    result = self.__taskManager.getTasks( [ 'TaskID', 'Status' ], condDict, -1, '' )
    if not result['OK']:
      return result

    tasks = result['Value']

    self.log.info( '%d tasks will be refreshed' % len(tasks) )

    for task in tasks:
      taskID = task[0]
      status = task[1]

      if status in ['Ready', 'Processing', 'Finished']:
        result = self.__refreshTask( taskID )
        if not result['OK']:
          return result
        self.log.info( 'Task %d status is refreshed' % taskID )

      elif status in ['Rescheduling']:
        result = self.__rescheduleTask( taskID )
        if not result['OK']:
          return result
        self.log.info( 'Task %d is being rescheduled' % taskID )

      elif status in ['Deleting']:
        result = self.__deleteTask( taskID )
        if not result['OK']:
          return result
        self.log.info( 'Task %d is being deleted' % taskID )

    return S_OK()

  def __refreshTask( self, taskID ):
    return self.__taskManager.refreshTaskStatus( taskID )

  def __rescheduleTask( self, taskID ):
    return self.__taskManager.rescheduleTask( taskID )

  def __deleteTask( self, taskID ):
    return self.__taskManager.deleteTask( taskID )
