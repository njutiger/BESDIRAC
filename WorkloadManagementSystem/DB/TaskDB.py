#

import json

from DIRAC                import gConfig, S_OK, S_ERROR, Time
from DIRAC.Core.Base.DB   import DB


__RCSID__ = "$Id: TaskDB.py 1 2015-01-11 11:39:29 zhaoxh@ihep.ac.cn $"

class TaskDB( DB ):

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'TaskDB', 'WorkloadManagement/TaskDB', maxQueueSize )

    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ] )

  def __initializeDB( self ):
    """
    Create the tables
    """
    result = self._query( "show tables" )
    if not result[ 'OK' ]:
      return result

    tablesInDB = [ t[0] for t in result[ 'Value' ] ]
    tablesToCreate = {}
    self.__tablesDesc = {}

    self.__tablesDesc[ 'Task' ] = { 'Fields' : { 'TaskID'       : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                 'TaskName'     : 'VARCHAR(128) NOT NULL DEFAULT "unknown"',
                                                 'CreationTime' : 'DATETIME NOT NULL',
                                                 'Status'       : 'VARCHAR(64) NOT NULL DEFAULT "Unknown"',
                                                 'Owner'        : 'VARCHAR(64) NOT NULL DEFAULT "unknown"',
                                                 'OwnerDN'      : 'VARCHAR(255) NOT NULL DEFAULT "unknown"',
                                                 'OwnerGroup'   : 'VARCHAR(128) NOT NULL DEFAULT "unknown"',
                                                 'Site'         : 'VARCHAR(512) NOT NULL DEFAULT "ANY"',
                                                 'JobGroup'     : 'VARCHAR(512) NOT NULL DEFAULT ""',
                                                 'Progress'     : 'VARCHAR(128) NOT NULL DEFAULT "{}"',
                                                 'Info'         : 'VARCHAR(4096) NOT NULL DEFAULT "{}"',
                                               },
                                    'PrimaryKey' : 'TaskID',
                                    'Indexes': { 'TaskIDIndex'       : [ 'TaskID' ],
                                                 'CreationTimeIndex' : [ 'CreationTime' ],
                                                 'StatusIndex'       : [ 'Status' ],
                                                 'OwnerIndex'        : [ 'Owner' ],
                                                 'OwnerDNIndex'      : [ 'OwnerDN' ],
                                                 'OwnerGroupIndex'   : [ 'OwnerGroup' ],
                                               }
                                  }

    self.__tablesDesc[ 'TaskHistory' ] = { 'Fields' : { 'TaskHistoryID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                        'TaskID'        : 'BIGINT UNSIGNED NOT NULL DEFAULT 0',
                                                        'Status'        : 'VARCHAR(64) NOT NULL DEFAULT "Unknown"',
                                                        'StatusTime'    : 'DATETIME NOT NULL',
                                                        'Discription'   : 'VARCHAR(128) NOT NULL DEFAULT ""',
                                                      },
                                           'PrimaryKey' : 'TaskHistoryID',
                                           'Indexes': { 'TaskHistoryIDIndex' : [ 'TaskHistoryID' ],
                                                        'TaskIDIndex'        : [ 'TaskID' ],
                                                      }
                                         }

    self.__tablesDesc[ 'TaskJob' ] = { 'Fields' : { 'TaskJobID'    : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                    'TaskID'       : 'BIGINT UNSIGNED NOT NULL DEFAULT 0',
                                                    'JobID'        : 'BIGINT UNSIGNED NOT NULL DEFAULT 0',
                                                    'Info'         : 'VARCHAR(4096) NOT NULL DEFAULT "{}"',
                                                  },
                                       'PrimaryKey' : 'TaskJobID',
                                       'Indexes': { 'TaskJobIDIndex' : [ 'TaskJobID' ],
                                                    'TaskIDIndex'    : [ 'TaskID' ],
                                                    'JobIDIndex'     : [ 'JobID' ],
                                                  }
                                     }

    for tableName in self.__tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.__tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  def createTask( self, taskName, status, owner, ownerDN, ownerGroup, taskInfo ):
    taskAttrNames = ['TaskName', 'CreationTime', 'Status', 'Owner', 'OwnerDN', 'OwnerGroup', 'Info']
    taskAttrValues = [taskName, Time.dateTime(), status, owner, ownerDN, ownerGroup, json.dumps(taskInfo)]

    result = self.insertFields( 'Task', taskAttrNames, taskAttrValues )
    if not result['OK']:
      self.log.error( 'Can not create new task', result['Message'] )
      return result

    if 'lastRowId' not in result:
      return S_ERROR( 'Failed to retrieve a new ID for task' )

    taskID = int( result['lastRowId'] )

    self.log.info( 'TaskDB: New TaskID served "%s"' % taskID )

    return S_OK( taskID )

  def insertTaskHistory( self, taskID, status, discription = '' ):
    taskHistoryAttrNames = ['TaskID', 'Status', 'StatusTime', 'Discription']
    taskHistoryAttrValues = [taskID, status, Time.dateTime(), discription]

    result = self.insertFields( 'TaskHistory', taskHistoryAttrNames, taskHistoryAttrValues )
    if not result['OK']:
      self.log.error( 'Can not insert task history', result['Message'] )

    return result

  def addTaskJob( self, taskID, jobID, jobInfo ):
    taskJobAttrNames = ['TaskID', 'JobID', 'Info']
    taskJobAttrValues = [taskID, jobID, json.dumps(jobInfo)]

    result = self.insertFields( 'TaskJob', taskJobAttrNames, taskJobAttrValues )
    if not result['OK']:
      self.log.error( 'Can not add job to task', result['Message'] )

    return result

  def updateTaskStatus( self, taskID, status ):
    condDict = { 'TaskID': taskID }
    taskAttrNames = ['Status']
    taskAttrValues = [status]

    result = self.updateFields( 'Task', taskAttrNames, taskAttrValues, condDict )
    if not result['OK']:
      self.log.error( 'Can not update task status', result['Message'] )

    return result

  def updateTaskProgress( self, taskID, progress ):
    condDict = { 'TaskID': taskID }
    taskAttrNames = ['Progress']
    taskAttrValues = [json.dumps(progress)]

    result = self.updateFields( 'Task', taskAttrNames, taskAttrValues, condDict )
    if not result['OK']:
      self.log.error( 'Can not update task progress', result['Message'] )

    return result

  def getTasks( self, outFields, limit ):
    result = self.getFields( 'Task', outFields, limit = limit )
    if not result['OK']:
      self.log.error( 'Can not get task list', result['Message'] )
      return result

    return S_OK( result['Value'] )

  def getTask( self, taskID, outFields ):
    condDict = { 'TaskID': taskID }
    result = self.getFields( 'Task', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get task', result['Message'] )
      return result

    if not result['Value']:
      self.log.error( 'Task ID %d not found' % taskID )
      return S_ERROR( 'Task ID %d not found' % taskID )

    return S_OK( result['Value'][0] )

  def getTaskStatus( self, taskID ):
    outFields = ( 'Status', )
    result = self.getTask( taskID, outFields )
    if not result['OK']:
      self.log.error( 'Can not get task status', result['Message'] )
      return result

    return S_OK( result['Value'][0] )

  def getTaskInfo( self, taskID ):
    outFields = ( 'Info', )
    result = self.getTask( taskID, outFields )
    if not result['OK']:
      self.log.error( 'Can not get task info', result['Message'] )
      return result

    return S_OK( json.loads( result['Value'][0] ) )

  def getTaskJobs( self, taskID ):
    condDict = { 'TaskID': taskID }
    outFields = ( 'JobID', )
    result = self.getFields( 'TaskJob', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get task jobs', result['Message'] )
      return result

    return S_OK( [ i[0] for i in  result['Value'] ] )

  def getJobInfo( self, jobID ):
    condDict = { 'JobID': jobID }
    outFields = ( 'Info', )
    result = self.getFields( 'TaskJob', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get job info', result['Message'] )
      return result

    if not result['Value']:
      self.log.error( 'Job info %d not found' % jobID )
      return S_ERROR( 'Job info %d not found' % jobID )

    return S_OK( json.loads( result['Value'][0][0] ) )
