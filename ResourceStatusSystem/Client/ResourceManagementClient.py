""" ResourceManagementClient

  ResourceManagementClient for BESDIRAC. 

"""

from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient as DIRACResourceManagementClient


__RCSID__ = '$Id:  $'


class ResourceManagementClient( DIRACResourceManagementClient ):
  """
    ResourceManagementClient inherits from DIRAC's ResourceManagementClient.
  """

  # JobCache Methods ...........................................................

  def selectJobCache( self, site = None, maskStatus = None, efficiency = None,
                      running = None, waiting = None, done = None, failed = None, completed = None,
                      stalled = None, status = None, lastCheckTime = None, meta = None ):
    """
    Gets from JobCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site element
      **maskStatus** - `[, string, list ]`
        maskStatus for the site
      **efficiency** - `[, float, list ]`
        job efficiency ( successful / total )
      **running** - `[, integer, list ]`
        number of running jobs
      **waiting** - `[, integer, list ]`
        number of waiting jobs
      **done** - `[, integer, list ]`
        number of done jobs
      **failed** - `[, integer, list ]`
        number of failed jobs
      **completed** - `[, integer, list ]`
        number of completed jobs
      **stalled** - `[, integer, list ]`
        number of stalled jobs
      **status** - `[, string, list ]`
        status for the site computed
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp setting last time the result was checked
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'select', 'JobCache', locals() )


  def deleteJobCache( self, site = None, maskStatus = None, efficiency = None,
                      running = None, waiting = None, done = None, failed = None, completed = None,
                      stalled = None, status = None, lastCheckTime = None, meta = None  ):
    """
    Deletes from JobCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site element
      **maskStatus** - `[, string, list ]`
        maskStatus for the site
      **efficiency** - `[, float, list ]`
        job efficiency ( successful / total )
      **running** - `[, integer, list ]`
        number of running jobs
      **waiting** - `[, integer, list ]`
        number of waiting jobs
      **done** - `[, integer, list ]`
        number of done jobs
      **failed** - `[, integer, list ]`
        number of failed jobs
      **completed** - `[, integer, list ]`
        number of completed jobs
      **stalled** - `[, integer, list ]`
        number of stalled jobs
      **status** - `[, string, list ]`
        status for the site computed
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp setting last time the result was checked
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'delete', 'JobCache', locals() )


  def addOrModifyJobCache( self, site = None, maskStatus = None, efficiency = None,
                           running = None, waiting = None, done = None, failed = None, completed = None,
                           stalled = None, status = None, lastCheckTime = None, meta = None  ):
    """
    Adds or updates-if-duplicated to StorageCache. Using `site` to query
    the database, decides whether to insert or update the table.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site element
      **maskStatus** - `[, string, list ]`
        maskStatus for the site
      **efficiency** - `[, float, list ]`
        job efficiency ( successful / total )
      **running** - `[, integer, list ]`
        number of running jobs
      **waiting** - `[, integer, list ]`
        number of waiting jobs
      **done** - `[, integer, list ]`
        number of done jobs
      **failed** - `[, integer, list ]`
        number of failed jobs
      **completed** - `[, integer, list ]`
        number of completed jobs
      **stalled** - `[, integer, list ]`
        number of stalled jobs
      **status** - `[, string, list ]`
        status for the site computed
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp setting last time the result was checked
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'JobCache', locals() )


  # StorageCache methods ......................................................
  
  def selectStorageCache( self, sE = None, occupied = None, free = None,
                          usage = None, lastCheckTime = None, meta = None ):
    """ 
    Gets from StorageCache all rows that match the parameters given.
     
    :Parameters:
      **sE** - `[, string, list ]`
        name of se
      **occupied** - `[, integer, list ]`
        occupied storage of se
      **free** - `[, integer, list ]`
        free storage of se
      **usage** - `[, float, list ]`
        usage rate of se
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'select', 'StorageCache', locals() )
  
  def deleteStorageCache( self, sE = None, occupied = None, free = None,
                          usage = None, lastCheckTime = None, meta = None ):
    """
    Deletes from StorageCache all rows that match the parameters given.
    
     :Parameters:
      **sE** - `[, string, list ]`
        name of se
      **occupied** - `[, integer, list ]`
        occupied storage of se
      **free** - `[, integer, list ]`
        free storage of se
      **usage** - `[, float, list ]`
        usage rate of se
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'delete', 'StorageCache', locals() )
  
  def addOrModifyStorageCache( self, sE = None, occupied = None, free = None,
                               usage = None, lastCheckTime = None, meta =None ):
    """
    Adds or updates-if-duplicated to StorageCache. Using `sE` to query
    the database, decides whether to insert or update the table.
    
     :Parameters:
      **sE** - `[, string, list ]`
        name of se
      **occupied** - `[, integer, list ]`
        occupied storage of se
      **free** - `[, integer, list ]`
        free storage of se
      **usage** - `[, float, list ]`
        usage rate of se
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'StorageCache', locals() )


  # WorkNodeCache methods ......................................................
  
  def selectWorkNodeCache( self, host = None, site = None, done = None,
                       failed = None, efficiency = None, lastCheckTime = None, meta = None ):
    """
    Gets from WorkNodeCache all rows that match the parameters given.
    
    :Parameters:
      **host** - `[, string, list ]`
        name of the host
      **site** - `[, string, list ]`
        name of the site
      **done** - `[, integer, list ]`
        number for the done jobs
      **failed** - `[, integer, list ]`
        number for the failed jobs
      **efficiency** - `[, float, list ]`
        job efficiency
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'select', 'WorkNodeCache', locals() )


  def deleteWorkNodeCache( self, host = None, site = None, done = None,
                       failed = None, efficiency = None, lastCheckTime = None, meta = None ):
    """
    Deletes from WorkNodeCache all rows that match the parameters given.
    
    :Parameters:
      **host** - `[, string, list ]`
        name of the host
      **site** - `[, string, list ]`
        name of the site
      **done** - `[, integer, list ]`
        number for the done jobs
      **failed** - `[, integer, list ]`
        number for the failed jobs
      **efficiency** - `[, float, list ]`
        job efficiency
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'delete', 'WorkNodeCache', locals() )


  def addOrModifyWorkNodeCache( self, host = None, site = None,  done = None,
                       failed = None, efficiency = None, lastCheckTime = None, meta = None ):
    """
    Adds or updates-if-duplicated to WorkNodeCache. Using `host` to query
    the database, decides whether to insert or update the table.
    
    :Parameters:
      **host** - `[, string, list ]`
        name of the host
      **site** - `[, string, list ]`
        name of the site
      **done** - `[, integer, list ]`
        number for the done jobs
      **failed** - `[, integer, list ]`
        number for the failed jobs
      **efficiency** - `[, float, list ]`
        job efficiency
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'WorkNodeCache', locals() )


  # SAMResult methods ......................................................
 
  def selectSAMResult( self, elementName = None, testType = None, elementType = None,
                          status = None, log = None, jobID = None, submissionTime = None,
                          completionTime = None, applicationTime = None, lastCheckTime = None, meta = None ):
    """
    Gets from SAMResult all rows that match the parameters given.
    
    :Parameters:
      **elementName** - `[, string, list ]`
        name of the element
      **testType** - `[, string, list ]`
        type of the test
      **elementType** - `[, string, list ]`
        type of the element
      **status** - `[, string, list ]`
        status for the executed test
      **log** - `[, string, list ]`
        log for the executed test
      **jobID** - `[, integer, list ]`
        the test job id
      **submissionTime** - `[, datetime, list ]`
        submission time for the test job
      **completionTime** - `[, datetime, list ]`
        completion time for the test job
      **applicationTime** - `[, float, list ]`
        real running time for the test
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'select', 'SAMResult', locals() )


  def deleteSAMResult( self, elementName = None, testType = None, elementType = None,
                          status = None, log = None, jobID = None, submissionTime = None,
                          completionTime = None, applicationTime = None, lastCheckTime = None, meta = None ):
    """
    Deletes from SAMResult all rows that match the parameters given.
    
    :Parameters:
      **elementName** - `[, string, list ]`
        name of the element
      **testType** - `[, string, list ]`
        type of the test
      **elementType** - `[, string, list ]`
        type of the element
      **status** - `[, string, list ]`
        status for the executed test
      **log** - `[, string, list ]`
        log for the executed test
      **jobID** - `[, integer, list ]`
        the test job id
      **submissionTime** - `[, datetime, list ]`
        submission time for the test job
      **completionTime** - `[, datetime, list ]`
        completion time for the test job
      **applicationTime** - `[, float, list ]`
        real running time for the test
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'delete', 'SAMResult', locals() )


  def addOrModifySAMResult( self, elementName = None, testType = None, elementType = None,
                               status = None, log = None, jobID = None, submissionTime = None,
                               completionTime = None, applicationTime = None, lastCheckTime = None, meta = None ):
    """
    Adds or updates-if-duplicated to SAMResult. Using `elementName`
    and `testType` to query the database, decides whether to insert or update
    the table.
    
    :Parameters:
      **elementName** - `[, string, list ]`
        name of the element
      **testType** - `[, string, list ]`
        type of the test
      **elementType** - `[, string, list ]`
        type of the element
      **status** - `[, string, list ]`
        status for the executed test
      **log** - `[, string, list ]`
        log for the executed test
      **jobID** - `[, integer, list ]`
        the test job id
      **submissionTime** - `[, datetime, list ]`
        submission time for the test job
      **completionTime** - `[, datetime, list ]`
        completion time for the test job
      **applicationTime** - `[, float, list ]`
        real running time for the test
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'SAMResult', locals() )


  # ResourceSAMStatus methods ......................................................
  
  def selectResourceSAMStatus( self, vO = None, elementName = None,
                               elementType = None, tests = None, status = None,
                               lastCheckTime = None, meta = None ):
    """
    Gets from ResourceSAMStatus all rows that match the parameters given.
    
    :Parameters:
      **vO** - `[, string, list]`
        name of the vo
      **elementName** - `[, string, list ]`
        name of the element
      **elementType** - `[, string, list ]`
        type of the element
      **tests** - `[, string, list ]`
        tests which executed for the element
      **status** - `[, string, list ]`
        sam status of the element
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'select', 'ResourceSAMStatus', locals() )


  def deleteResourceSAMStatus( self, vO = None, elementName = None,
                               elementType = None, tests = None, status = None,
                               lastCheckTime = None, meta = None ):
    """
    Deletes from ResourceSAMStatus all rows that match the parameters given.
    
    :Parameters:
      **vO** - `[, string, list]`
        name of the vo
      **elementName** - `[, string, list ]`
        name of the element
      **elementType** - `[, string, list ]`
        type of the element
      **tests** - `[, string, list ]`
        tests which executed for the element
      **status** - `[, string, list ]`
        sam status of the element
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'delete', 'ResourceSAMStatus', locals() )


  def addOrModifyResourceSAMStatus( self, vO = None, elementName = None,
                                    elementType = None, tests = None, status = None,
                                    lastCheckTime = None,meta = None ):
    """
    Adds or updates-if-duplicated to ResourceSAMStatus. Using `elementName`
    to query the database, decides whether to insert or update the table.
    
    :Parameters:
      **vO** - `[, string, list]`
        name of the vo
      **elementName** - `[, string, list ]`
        name of the element
      **elementType** - `[, string, list ]`
        type of the element
      **tests** - `[, string, list ]`
        tests which executed for the element
      **status** - `[, string, list ]`
        sam status of the element
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'ResourceSAMStatus', locals() )
    
    
  # SiteSAMStatus methods ......................................................
  
  def selectSiteSAMStatus( self, vO = None, site = None, siteType = None, status = None,
                                cEStatus = None, sEStatus = None, lastCheckTime = None, meta = None ):
    """
    Gets from SiteSAMStatus all rows that match the parameters given.
    
    :Parameters:
      **vO** - `[, string, list]`
        name of the vo
      **site** - `[, string, list ]`
        name of the site
      **siteType** - `[, string, list ]`
        type of the site
      **status** - `[, string, list ]`
        sam status of the site
      **cEStatus** - `[, string, list ]`
        ce sam status of the site
      **sEStatus** - `[, string, list ]`
        se sam status of the site
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'select', 'SiteSAMStatus', locals() )


  def deleteSiteSAMStatus( self, vO = None, site = None, siteType = None, status = None,
                                cEStatus = None, sEStatus = None, lastCheckTime = None, meta = None ):
    """
    Deletes from SiteSAMStatus all rows that match the parameters given.
    
    :Parameters:
      **vO** - `[, string, list]`
        name of the vo
      **site** - `[, string, list ]`
        name of the site
      **siteType** - `[, string, list ]`
        type of the site
      **status** - `[, string, list ]`
        sam status of the site
      **cEStatus** - `[, string, list ]`
        ce sam status of the site
      **sEStatus** - `[, string, list ]`
        se sam status of the site
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    return self._query( 'delete', 'SiteSAMStatus', locals() )


  def addOrModifySiteSAMStatus( self, vO = None, site = None, siteType = None, status = None,
                                     cEStatus = None, sEStatus = None, lastCheckTime = None, meta = None ):
    """
    Adds or updates-if-duplicated to SiteSAMStatus. Using `site`
    to query the database, decides whether to insert or update the table.
    
    :Parameters:
      **vO** - `[, string, list]`
        name of the vo
      **site** - `[, string, list ]`
        name of the site
      **siteType** - `[, string, list ]`
        type of the site
      **status** - `[, string, list ]`
        sam status of the site
      **cEStatus** - `[, string, list ]`
        ce sam status of the site
      **sEStatus** - `[, string, list ]`
        se sam status of the site
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp from which the result is effective
      **meta** - `[, dict ]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.
    """
    
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'SiteSAMStatus', locals() )


  # get history methods ......................................................
  
  def selectSAMResultLog( self, elementName = None, testType = None, elementType = None,
                          status = None, log = None, jobID = None, submissionTime = None,
                          completionTime = None, applicationTime = None, lastCheckTime = None, meta = None ):
    return self._query( 'select', 'SAMResultLog', locals() )


  def selectResourceSAMStatusLog( self, vO = None, elementName = None, elementType = None,
                                         tests = None, status = None, lastCheckTime = None, meta = None ):
    return self._query( 'select', 'ResourceSAMStatusLog', locals() )


  def selectSiteSAMStatusLog( self, vO = None, site = None, siteType = None, status = None,
                                cEStatus = None, sEStatus = None, lastCheckTime = None, meta = None ):
    return self._query( 'select', 'SiteSAMStatusLog', locals() )
