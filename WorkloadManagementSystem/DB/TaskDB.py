#

import json

from DIRAC                import gConfig, S_OK, S_ERROR, Time
from DIRAC.Core.Base.DB   import DB


__RCSID__ = "$Id: TaskDB.py 1 2015-01-11 11:39:29 zhaoxh@ihep.ac.cn $"

class TaskDB( DB ):

  def __init__( self ):
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

  def createTask( self, taskName, owner, ownerDN, ownerGroup, taskInfo ):
    status = 'Created'

    taskAttrNames = ['TaskName', 'CreationTime', 'Status', 'Owner', 'OwnerDN', 'OwnerGroup', 'Info']
    taskAttrValues = [jdl, Time.dateTime(), status, owner, ownerDN, ownerGroup, json.dumps(taskInfo)]

    result = self.insertFields( 'Task' , taskAttrNames, taskAttrValues )
    if not result['OK']:
      self.log.error( 'Can not create new task', result['Message'] )
      return result

    if 'lastRowId' not in result:
      return S_ERROR( 'Failed to retrieve a new ID for task' )

    taskID = int( result['lastRowId'] )

    self.log.info( 'TaskDB: New TaskID served "%s"' % taskID )

    self.insertTaskHistory( taskID, status, 'New task created' )

    return S_OK( taskID )

  def insertTaskHistory( self, taskID, status, discription = '' ):
    taskHistoryAttrNames = ['TaskID', 'Status', 'StatusTime', 'Discription']
    taskHistoryAttrValues = [taskID, status, Time.dateTime(), discription]

    result = self.insertFields( 'TaskHistory' , taskHistoryAttrNames, taskHistoryAttrValues )
    if not result['OK']:
      self.log.error( 'Can not insert task history', result['Message'] )

    return result

  def addTaskJob( self, taskID, jobID, jobInfo ):
    taskJobAttrNames = ['TaskID', 'JobID', 'Info']
    taskJobAttrValues = [taskID, jobID, json.dump(jobInfo)]

    result = self.insertFields( 'TaskJob' , taskJobAttrNames, taskJobAttrValues )
    if not result['OK']:
      self.log.error( 'Can not add job to task', result['Message'] )

    return result
