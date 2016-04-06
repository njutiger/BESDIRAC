from datetime import datetime
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ResourceStatusSystem.SAMSystem.TestExecutor import TestExecutor
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient


class StatusEvaluator(object):
    def __init__(self, sites, tests, apis = {}):
        self.__sites = sites
        self.__resources = []
        self.__siteStatus = []
        self.__resourceStatus = []
        self.__tests = tests
        self.__intailizeResource()
        
        self.states = { 'OK' : 3, 'Warn' : 2, 'Bad' : 1, 'Unknown' : 0 }
        
        if 'ResourceManagementClient' in apis:
            self.rmClient = apis[ 'ResourceManagementClient' ]
        else:
            self.rmClient = ResourceManagementClient()
    
    
    def __intailizeResource(self):
        for siteDict in self.__sites:
            siteName = siteDict[ 'SiteName' ]
            siteType = siteDict[ 'SiteType' ]
            
            if siteType == 'CLOUD':
                self.__resources.append({ 'ElementName' : siteName, 'ElementType' : 'CLOUD' })
            else:
                for ce in siteDict[ 'ComputingElement' ]:
                    self.__resources.append({ 'ElementName' : ce, 'ElementType' : 'ComputingElement' })
                    
            for se in siteDict[ 'StorageElement' ]:
                self.__resources.append({ 'ElementName' : se, 'ElementType' : 'StorageElement' })
        
     
    def __statusRule(self, statusList):
        for status in statusList:
            if status == "":
                statusList.remove(status)
                
        if len(statusList) == 0:
            return S_OK("")
        
        statusSum = 0
        
        for status in statusList:
            try:
                statusVal = self.states[ status ]
            except KeyError:
                gLogger.error('%s is not valid status.' % status)
                return S_ERROR('%s is not valid status.' % status)
            
            statusSum = statusSum + statusVal
            
        finalStatusVal = statusSum / len(statusList)
        
        if finalStatusVal >= 2.8:
            finalStatus = 'OK'
        elif 1.2 < finalStatusVal < 2.8:
            finalStatus = 'Warn'
        elif 0 < finalStatusVal <= 1.2:
            finalStatus = 'Bad'
        else:
            finalStatus = 'Unknown'
            
        return S_OK(finalStatus)
    
    
    def evaluateResourceStatus(self, testResults):
        resourceStatusRet = {}
        
        for resource in self.__resources:
            elementName = resource[ 'ElementName' ]
            elementType = resource[ 'ElementType' ]
            
            if elementName in testResults:
                tests = ','.join(testResults[ elementName ].keys())
                
                samStatus = self.__statusRule(testResults[ elementName ].values())         
                if not samStatus[ 'OK' ]:
                    return samStatus
                samStatus = samStatus[ 'Value' ]
                
                resourceStatusRet[ elementName ] = samStatus
                
                self.__resourceStatus.append({
                                              'ElementName' : elementName,
                                              'ElementType' : elementType,
                                              'Tests' :tests,
                                              'SAMStatus' : samStatus
                                              })
                
        return S_OK(resourceStatusRet)
    
    
    def evaluateSiteStatus(self, resourceStatus):
        for siteDict in self.__sites:
            siteName = siteDict[ 'SiteName' ]
            siteType = siteDict[ 'SiteType' ]
            
            if siteType == ' CLOUD':
                ceStatusList = [ resourceStatus.get(siteName, "") ]
            else:
                ceStatusList = [ resourceStatus.get(ce, "") for ce in siteDict[ 'ComputingElement' ] ]
            ceStatus = self.__statusRule(ceStatusList)
            if not ceStatus[ 'OK' ]:
                return ceStatus
            ceStatus = ceStatus[ 'Value' ]
                
            seStatusList = [ resourceStatus.get(se, "") for se in siteDict[ 'StorageElement' ] ]
            seStatus = self.__statusRule(seStatusList)
            if not seStatus[ 'OK' ]:
                return seStatus
            seStatus = seStatus[ 'Value' ]
            
            statusList = []
            statusList.extend(ceStatusList)
            statusList.extend(seStatusList)
            samStatus = self.__statusRule(statusList)
            if not samStatus[ 'OK' ]:
                return samStatus
            samStatus = samStatus[ 'Value' ]
            
            self.__siteStatus.append({
                                      'Site' : siteName,
                                      'SiteType' : siteType,
                                      'SAMStatus' : samStatus,
                                      'CEStatus' : ceStatus,
                                      'SEStatus' : seStatus
                                      })
        
        return S_OK()
            
    def __storeSAMStatus(self, lastUpdateTime):
        for resourceStatusDict in self.__resourceStatus:
            resQuery = self.rmClient.addOrModifyResourceSAMStatusCache(
                                                                       resourceStatusDict[ 'ElementName' ],
                                                                       resourceStatusDict[ 'ElementType' ],
                                                                       resourceStatusDict[ 'Tests' ],
                                                                       resourceStatusDict[ 'SAMStatus' ],
                                                                       lastUpdateTime
                                                                       )
            if not resQuery[ 'OK' ]:
                return resQuery
            
        for siteStatusDict in self.__siteStatus:
            resQuery = self.rmClient.addOrModifySiteSAMStatusCache(
                                                                   siteStatusDict[ 'Site' ],
                                                                   siteStatusDict[ 'SiteType' ],
                                                                   siteStatusDict[ 'SAMStatus' ],
                                                                   siteStatusDict[ 'CEStatus' ],
                                                                   siteStatusDict[ 'SEStatus' ],
                                                                   lastUpdateTime
                                                                   )
            if not resQuery[ 'OK' ]:
                return resQuery
            
        return S_OK()
            
        
    def evaluate(self):       
        lastUpdateTime = datetime.utcnow().replace(microsecond = 0)
        
        executor = TestExecutor(self.__tests, self.__resources)
        exeRes = executor.execute()
        if not exeRes[ 'OK' ]:
            gLogger.error('SAM Test execute failed.')
            return exeRes
        testResults = exeRes[ 'Value' ]
        
        uniformResults = {}
        
        for testDict in testResults:
            elementName = testDict['ElementName' ]
            testType = testDict[ 'TestType' ]
            status = testDict[ 'Status' ]
            
            if elementName not in uniformResults:
                uniformResults[ elementName ] = {}          
            uniformResults[ elementName ][ testType ] = status
            
        resourceStatus = self.evaluateResourceStatus(uniformResults)
        if not resourceStatus[ 'OK' ]:
            return resourceStatus
        resourceStatus = resourceStatus[ 'Value' ]
        
        siteStatus = self.evaluateSiteStatus(resourceStatus)
        if not siteStatus[ 'OK' ]:
            return siteStatus
            
        storeRes = self.__storeSAMStatus(lastUpdateTime)
        if not storeRes[ 'OK' ]:
            return storeRes
        
        return S_OK()
        
                
            
            
            
            

