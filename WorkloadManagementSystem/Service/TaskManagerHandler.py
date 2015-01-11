
from types import DictType, FloatType, IntType, ListType, LongType, StringTypes, TupleType, UnicodeType

import time

# DIRAC
from DIRAC                                import gConfig, gLogger, S_ERROR, S_OK, Time
from DIRAC.Core.DISET.RequestHandler      import RequestHandler, getServiceOption

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
     self.rpcProperties       = credDict[ 'properties' ]

  types_createTask = [ StringTypes, DictType ]
  def export_createTask( self, taskName, taskInfo ):
    """ Create a new task
    """
    credDict = self.getRemoteCredentials()
    owner = credDict[ 'username' ]
    ownerDN = credDict[ 'DN' ]
    ownerGroup = credDict[ 'group' ]
    return gTaskDB.createTask( taskName, owner, ownerDN, ownerGroup, taskInfo )

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
