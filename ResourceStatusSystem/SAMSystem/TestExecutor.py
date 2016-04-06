import Queue
from DIRAC import S_OK, gLogger
from DIRAC.ResourceStatusSystem.Utilities import Utils
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient



class TestExecutor(object):
    def __init__(self, tests, elements, apis = {}):
        self.__tests = {}
        self.__elements = {}
        self.__testResults = []
        self.__initializeTests(tests)
        self.__initializeElements(elements)
        
        if 'ResourceManagementClient' in apis:
            self.rmClient = apis[ 'ResourceManagementClient' ]
        else:
            self.rmClient = ResourceManagementClient()
        
    
    def __initializeTests(self, tests):
        for testType, testArgs in tests.items():
            module = testArgs[ 'module' ]
            executable = testArgs[ 'executable' ]
            timeout = testArgs[ 'timeout' ]
            
            testClass = self.__loadTestClass(module)
            testObj = testClass(executable, timeout)
            
            self.__tests[ testType ] = testObj
            
        
    def __initializeElements(self, elements):
        for element in elements:
            elementName = element[ 'ElementName' ]
            elementType = element[ 'ElementType' ]
            
            if elementType not in self.__elements:
                self.__elements[ elementType ] = []
                
            self.__elements[ elementType ].append(elementName)
        
    
    @staticmethod
    def __loadTestClass(moduleName):
        _module_pre = 'DIRAC.ResourceStatusSystem.SAMSystem.SAMTests.'
        
        try:
            testModule = Utils.voimport(_module_pre + moduleName)
        except ImportError, e:
            gLogger.error("Unable to import %s, %s" % (_module_pre + moduleName, e))
            raise ImportError
        
        testClass = getattr(testModule, moduleName)
        
        return testClass
    
    
    def __storeTestResults(self):
        for testDict in self.__testResults:
            testDict.update(testDict.pop('TestInfo'))
            
            testDict[ 'CompletionTime' ] = testDict[ 'CompletionTime' ] or '0000-0-0'
            testDict[ 'AppliactionTime' ] = testDict[ 'ApplicationTime' ] or 0
                
            resQuery = self.rmClient.addOrModifySAMResultCache(
                                                                   testDict[ 'ElementName' ],
                                                                   testDict[ 'TestType' ],
                                                                   testDict[ 'ElementType' ],
                                                                   testDict[ 'Status' ],
                                                                   testDict[ 'Log' ],
                                                                   testDict[ 'JobID' ],
                                                                   testDict[ 'SubmissionTime' ],
                                                                   testDict[ 'CompletionTime' ],
                                                                   testDict[ 'ApplicationTime' ]
                                                                   )
            if not resQuery[ 'OK' ]:
                return resQuery
        
        return S_OK()
            
    
    def execute(self):
        self.submittedTestsQueue = Queue.Queue()
        
        for testType, testObj in self.__tests.items():
            for elementType in testObj.elementType:
                if elementType not in self.__elements:
                    continue
                
                for elementName in self.__elements[ elementType ]:
                    testDict = { 'ElementName' : elementName, 'TestType' : testType, 'ElementType' : elementType }
                    result = testObj.sendTest(elementName, elementType)
                    if not result[ 'OK' ]:
                        return result
                    testDict['TestInfo'] = result[ 'Value' ]
                    self.submittedTestsQueue.put(testDict)
        
        while not self.submittedTestsQueue.empty():
            testDict = self.submittedTestsQueue.get_nowait()
            testObj = self.__tests[ testDict[ 'TestType' ] ]
            jobID = testDict[ 'TestInfo' ][ 'JobID' ]
            submissionTime = testDict[ 'TestInfo' ][ 'SubmissionTime' ]
            result = testObj.getTestResult(jobID, submissionTime)
            if not result[ 'OK' ]:
                return result
            if result[ 'IsFinish' ]:
                testDict[ 'TestInfo' ].update(result[ 'Value' ])
                self.__testResults.append(testDict)
            else:
                self.submittedTestsQueue.put(testDict)
            self.submittedTestsQueue.task_done()
            
        self.submittedTestsQueue.join()
        
        storeRes = self.__storeTestResults()
        if not storeRes[ 'OK' ]:
            return storeRes
        return S_OK(self.__testResults)
                    
