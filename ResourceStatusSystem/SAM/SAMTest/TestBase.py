""" TestBase

  Base class for all tests.
  
"""

from abc   import ABCMeta,abstractmethod
from DIRAC import S_OK, S_ERROR, gLogger

__RCSID__ = '$Id:  $'

class TestBase( object ):
  """
    TestBase is a simple base class for all tests. Real test classes should
    implement its doTest and getTestResult method.
  """
  
  __metaclass__ = ABCMeta
    
  def __init__( self, args = None, apis = None ):
    self.apis = apis or {}
    self.args = args or {}
    self.__columns = ( 'ElementName', 'TestType', 'ElementType', 
                'Status', 'Log', 'JobID', 'SubmissionTime', 
                'CompletionTime', 'ApplicationTime' )
    self._testResult = {}
    self.testType = ''
    self.testGroup = ''
    self.elementType = ()
      
  
  def _updateResult( self, elementName, **kwds ):
    """
      update test result dictionary with the input kwds.
    """
    
    if elementName not in self._testResult:
      return S_ERROR( '%s for %s has not been executed.' % ( self.testType, elementName ) )
    else:
      resDict = self._testResult[ elementName ]
      for key, val in kwds.items():
        if key not in self.__columns:
          gLogger.warn( '%s is not a vaild key.' % key )
        else:
          resDict[ key ] = val
    return S_OK()
      
      
  @abstractmethod
  def doTest( self, elementName, elementType ):
    """
      to be extended by real tests.
    """
    
    resDict = {}
    for column in self.__columns:
      resDict[ column ] = None
    resDict[ 'TestType' ] = self.testType
    resDict[ 'ElementName' ] = elementName
    resDict[ 'ElementType' ] = elementType
    self._testResult[ elementName ] = resDict
      
      
  @abstractmethod
  def getTestResult( self, elementName ):
    """
      to be extended by real tests.
    """
    
    if elementName not in self._testResult:
      return S_ERROR( '%s for %s has not been executed.' % ( self.testType, elementName ) )