""" TestExecutor

  TestExecutor is the end-point to executor SAM tests. It loads the tests which
  need to be executed dynamically and executes all the tests. At last, it stores
  the test results to database.
"""

import Queue
from DIRAC                                                         import S_OK, gLogger
from DIRAC.ResourceStatusSystem.Utilities                          import Utils
from BESDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient


__RCSID__ = '$Id:  $'


class TestExecutor( object ):
  """ TestExecutor
  """

  def __init__( self, tests, elements, apis = None ):
    """ Constructor

    examples:
      >>> tests = { 'WMS-Test' : { 'module' : 'WMSTest',
                                                           'args' : { 'executable' : [ '/usr/bin/python',
                                                                           'wms_test.py' ], 'timeout' : 1800 } } }
      >>> elements = { 'ComputingElement' : [ 'chenj01.ihep.ac.cn' ],
                                       'StorageElement', [ 'IHEPD-USER' ],
                                       'CLOUD' : [ 'CLOUD.IHEP-OPENSTACK.cn' ] }
      >>> executor = TestExecutor( tests, elements )
      >>> executor1 = TestExecutor( tests, elements, { 'ResourceManagementClient' :
                                                                                                     ResourceManagementClient() } )

    :Parameters:
      **tests** - `dict`
        dictionary with tests to be executed. The test class is loaded according to the
        'module' key and instantiated with 'args' key.
      **elements** - `dict`
        the elements need to be tested. The elements is grouped by type.
      **apis** - 'dict'
        dictionary with clients to be used in the commands issued by the policies.
        If not defined, the commands will import them.
    """

    self.apis = apis or {}
    self.__tests = {}
    self.__elements = elements
    self.__initializeTests( tests )

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()


  def __initializeTests( self, tests ):
    """
      Instantiate the test class for each type of  tests and put the objects into
      dictionary self.__tests.
    """

    for testType, testArgs in tests.items():
      module = testArgs[ 'module' ]
      args = testArgs[ 'args' ]

      testClass = self.__loadTestClass( module )
      testObj = testClass( args, self.apis )

      self.__tests[ testType ] = testObj


  @staticmethod
  def __loadTestClass( moduleName ):
    """ Loads the test class according the input 'moduleName'. If  test class
    exists, return the class. Otherwise, throws ImportError exception.

    :Parameters:
      **moduleName** - `str`
        module name of the test.

    :return: class
    """

    _module_pre = 'DIRAC.ResourceStatusSystem.SAM.SAMTest.'

    try:
      testModule = Utils.voimport( _module_pre + moduleName )
    except ImportError, e:
      gLogger.error( "Unable to import %s, %s" % ( _module_pre + moduleName, e ) )
      raise ImportError

    testClass = getattr( testModule, moduleName )

    return testClass


  def __storeTestResults( self, results ):
    """
      store the test results.
    """
    for testDict in results:
      testDict[ 'CompletionTime' ] = testDict[ 'CompletionTime' ] or '0000-0-0'
      testDict[ 'AppliactionTime' ] = testDict[ 'ApplicationTime' ] or 0

      resQuery = self.rmClient.addOrModifySAMResult(
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


  def execute( self ):
    """ Main method which executes the tests and obtains the results. Use
    two loops to do all the work. In the first loop, execute all the tests for
    corresponding elements and put the executed tests into  executedTestsQueue.
    In the second loop, traverse executedTestsQueue to obtain test results.

    examples:
      >>> executor.execute()[ 'Value' ]
          { 'Records' : ( ( 'chenj01.ihep.ac.cn', 'WMS-Test', 'ComputingElement', 'OK', 
                                       'balabala', 1, '2016-5-8 00:00:00', '2016-5-8 00:05:23', 0.1234 ),
                                    ( 'chenj01.ihep.ac.cn', 'BOSS-Test', 'ComputingElement', 'Bad', 
                                       'balabala', 2, '2016-5-8 00:00:00', '0000-0-0', 0 ),
                                    ( 'IHEPD-USER', 'SE-Test', 'StorageElement', 'Bad', 
                                      'balabala', None, '2016-5-8 00:00:00', '0000-0-0', 0 ) ),
             'Columns' : ( 'ElementName', 'TestType', 'ElementType', 'Status',
                                      'Log', 'JobID', 'SubmissionTime', 'CompletionTime', 'ApplicationTime' ) }
    
    :return: S_OK( { 'Records' : `tuple`, 'Columns' : `tuple` } ) / S_ERROR
    """

    executedTestsQueue = Queue.Queue()
    results = []

    for testType, testObj in self.__tests.items():
      for elementType in testObj.elementType:
        for elementName in self.__elements.get( elementType, [] ):
          result = testObj.doTest( elementName, elementType )
          if not result[ 'OK' ]:
            return result
          executedTestsQueue.put( ( testType, elementName ) )

    while not executedTestsQueue.empty():
      testType, elementName = executedTestsQueue.get_nowait()
      result = self.__tests[ testType ].getTestResult( elementName )
      if not result[ 'OK' ]:
        return result
      result = result[ 'Value' ]
      if not result:
        executedTestsQueue.put( ( testType, elementName ) )
      else:
        results.append( result )
      executedTestsQueue.task_done()

    executedTestsQueue.join()

    storeRes = self.__storeTestResults( results )
    if not storeRes[ 'OK' ]:
      return storeRes

    records = []
    columns = results[ 0 ].keys()
    for result in results:
      record = [ result[ column ] for column in columns ]
      records.append( tuple( record ) )

    return S_OK( { 'Records' : tuple( records ), 'Columns' : tuple( columns ) } )
