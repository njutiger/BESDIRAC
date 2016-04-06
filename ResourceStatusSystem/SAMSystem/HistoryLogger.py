from datetime import datetime, timedelta
from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from ResourceStatusSystem.Service.PublisherHandler import rmClient


    

class HistoryLogger(object):
    
    def __init__(self):
        self.rmClient = ResourceManagementClient()
    
    
    def __logSAMResult(self, record):
        resQuery = self.rmClient.insertSAMResultHistory(
                                                        record[ 'ElementName' ],
                                                        record[ 'TestType' ],
                                                        record[ 'ElementType' ],
                                                        record[ 'Status' ],
                                                        record[ 'Log' ],
                                                        record[ 'JobID' ],
                                                        record[ 'SubmissionTime' ],
                                                        record[ 'CompletionTime' ],
                                                        record[ 'ApplicationTime' ]
                                                        )
        if not resQuery[ 'OK' ]:
            return resQuery
        
        return S_OK()
        
        
    def __logResourceSAMStatus(self, record):
        resQuery = self.rmClient.insertResourceSAMStatusHistory(
                                                                record[ 'ElementName' ],
                                                                record[ 'ElementType' ],
                                                                record[ 'Tests' ],
                                                                record[ 'SAMStatus' ],
                                                                record[ 'Interval' ],
                                                                record[ 'LastUpdateTime' ]
                                                                )
        if not resQuery[ 'OK' ]:
            return resQuery
        
        return S_OK()
        
        
    def __logSiteSAMStatus(self, record):
        resQuery = self.rmClient.insertSiteSAMStatusHistory(
                                                            record[ 'Site' ],
                                                            record[ 'SiteType' ],
                                                            record[ 'SAMStatus' ],
                                                            record[ 'CEStatus' ],
                                                            record[ 'SEStatus' ],
                                                            record[ 'Interval' ],
                                                            record[ 'LastUpdateTime' ]
                                                            )
        if not resQuery[ 'OK' ]:
            return resQuery
        
        return S_OK()
    
    
    def __getHistoryRecords(self, tableName, fromDate, toDate):
        if tableName == 'SiteSAMStatusHistory':
            return rmClient.selectSiteSAMStatusHistory(interval = 'hour', meta= {
                                                                                       'newer' : [ 'LastUpdateTime', fromDate ],
                                                                                       'older' : [ 'LastUpdateTime', toDate ]
                                                                                       })
            
        if tableName == 'ResourceSAMStatusHistory':
            return rmClient.selectResourceSAMStatusHistory(interval = 'hour', meta= {
                                                                                       'newer' : [ 'LastUpdateTime', fromDate ],
                                                                                       'older' : [ 'LastUpdateTime', toDate ]
                                                                                       })
            
        return S_ERROR('The table named %s does not exist or does not need to combine.' % tableName)        
        
        
    def __mergeResourceHistory(self, fromDate, toDate):
        history = self.__getHistoryRecords('ResourceSAMStatusHistory', fromDate, toDate)
        if not history[ 'OK' ]:
            return history
        records = list(history[ 'Value' ])
        columns = history[ 'Columns' ]
        records.sort(key = lambda x : x[ 0 ])
        
        mergeRes = []
        i = 0
        while i < len(records):
            recordDict = dict(zip(columns, records[ i ]))
            recordDict[ 'SAMStatus' ] = [ recordDict[ 'SAMStatus' ] ]
            for j in range(i + 1, len(records)):
                if records[ i ][ 1 ] == records[ j ][ 1 ]:
                    recordDict[ 'SAMStatus' ].append( records[ j ][ 4 ] )
                else:
                    break
            mergeRes.append(recordDict)
            i = j
            
        for recordDict in mergeRes:
            recordDict[ 'SAMStatus' ] = self.__mergeRule( recordDict[ 'SAMStatus' ] )
            
        return S_OK(mergeRes)
            
            
    def __mergeSiteHistory(self, fromDate, toDate):
        history = self.__getHistoryRecords('SiteSAMStatusHistory', fromDate, toDate)
        if not history[ 'OK' ]:
            return history
        records = list(history[ 'Value' ])
        columns = history[ 'Columns' ]
        records.sort(key = lambda x : x[ 0 ])
        
        mergeRes = []
        i = 0
        while i < len(records):
            recordDict = dict(zip(columns, records[ i ]))
            recordDict[ 'CEStatus' ] = [ recordDict[ 'CEStatus' ] ]
            recordDict[ 'SEStatus' ] = [ recordDict[ 'SEStatus' ] ]
            recordDict[ 'SAMStatus' ] = [ recordDict[ 'SAMStatus' ] ]
            for j in range(i + 1, len(records)):
                if records[ i ][ 1 ] == records[ j ][ 1 ]:
                    recordDict[ 'SAMStatus' ].append( records[ j ][ 3 ] )
                    recordDict[ 'CEStatus' ].append( records[ j ][ 4 ] )
                    recordDict[ 'SEStatus' ].append( records[ j ][ 5 ] )
                else:
                    break
            mergeRes.append(recordDict)
            i = j
            
        for recordDict in mergeRes:
            recordDict[ 'CEStatus' ] = self.__mergeRule( recordDict[ 'CEStatus' ] )
            recordDict[ 'SEStatus' ] = self.__mergeRule( recordDict[ 'SEStatus' ] )
            recordDict[ 'SAMStatus' ] = self.__mergeRule( recordDict[ 'SAMStatus' ] )
            
        return S_OK(mergeRes)
            
     
    def __mergeRule(self, statusList):
        pass
    
    
    def __merge(self, interval, fromDate, toDate):
        lastUpdateTime = datetime.utcnow().replace(microsecond = 0)
        
        resMergeRes = self.__mergeResourceHistory(fromDate, toDate)
        if not resMergeRes[ 'OK' ]:
            return resMergeRes
        resMergeRes = resMergeRes[ 'Value' ]
        
        siteMergeRes = self.__mergeSiteHistory(fromDate, toDate)
        if not siteMergeRes[ 'OK' ]:
            return siteMergeRes
        siteMergeRes = siteMergeRes[ 'Value' ]
        
        for record in resMergeRes:
            record[ 'Interval' ] = interval
            record[ 'LastUpdateTime' ] = lastUpdateTime
            queryRes = self.__logResourceSAMStatus(record)
            if not queryRes[ 'OK' ]:
                return queryRes
            
        for record in siteMergeRes:
            record[ 'Interval' ] = interval
            record[ 'LastUpdateTime' ] = lastUpdateTime
            queryRes = self.__logSiteSAMStatus(record)
            if not queryRes[ 'OK' ]:
                return queryRes
            
        return S_OK()
        
        
    def historyMerge(self, time):
        hour = time.hour
        weekday = time.weekDay()
        day = time.day
        month = time.month
        year = time.year
        
        toDateStr = '%d-%d-%d' % (year, month, day)
        toDate = datetime.strptime(toDateStr, '%Y-%m-%d')
        
        if 0 <= hour < 2:
            fromDate = toDate - timedelta(days = 1)
            result = self.__merge('day', fromDate, toDate)
            if not result[ 'OK' ]:
                return result
            
            if weekday == 0:
                fromDate = toDate - timedelta(days = 7)
                result = self.__merge('week', fromDate, toDate)
                if not result[ 'OK' ]:
                    return result
                
            if day == 1:
                if month == 1:
                    fromDateStr = '%d-%d-%d' % (year - 1, 12, day)
                else:
                    fromDateStr = '%d-%d-%d' % (year, month - 1, day)
                fromDate = datetime.strptime(fromDateStr, '%Y-%m-%d')
                result = self.__merge('month', fromDate, toDate)
                if not result[ 'OK' ]:
                    return result
                
                if month == 1:
                    fromDateStr = '%d-%d-%d' % (year - 1, month, day)
                    fromDate = datetime.strptime(fromDateStr, '%Y-%m-%d')
                    result = self.__merge('year', fromDate, toDate)
                    if not result[ 'OK' ]:
                        return result
                    
        return S_OK()
        
        
#    def store(self, tableName, record):
#        if tableName == 'SAMResultHistory':
#            return self.__logSAMResult(record)
#            
#        if tableName == 'ResourceSAMStatusHistory':
#            return self.__logResourceSAMStatus(record)
#            
#        if tableName == 'SiteSAMStatusHistory':
#            return self.__logSiteSAMStatus(record)
#        
#        return S_ERROR('The table named %s does not exist.' % tableName)
