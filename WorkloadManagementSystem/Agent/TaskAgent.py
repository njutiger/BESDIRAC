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
    condDict = { 'Active': int(True) }
    result = self.__taskManager.getTasks( [ 'TaskID' ], condDict, -1, '' )
    if not result['OK']:
      return result

    tasks = result['Value']

    self.log.info( '%d tasks will be refreshed' % len(tasks) )

    for task in tasks:
      taskID = task[0]

      result = self.__taskManager.refreshTaskStatus( taskID )
      if not result['OK']:
        return result

      result = self.__taskManager.refreshTaskSites( taskID )
      if not result['OK']:
        return result

      result = self.__taskManager.refreshTaskJobGroup( taskID )
      if not result['OK']:
        return result

      self.log.info( 'Task %d is refreshed' % taskID )

    return S_OK()
