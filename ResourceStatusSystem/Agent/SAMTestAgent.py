''' SAMTestAgent

  This agent executes SAM tests and evaluates resources and sites SAM status.

'''

from DIRAC                                             import S_OK, gLogger
from DIRAC.Core.Base.AgentModule                       import AgentModule
from DIRAC.Core.DISET.RPCClient                        import RPCClient
from DIRAC.ResourceStatusSystem.Utilities              import CSHelpers
from DIRAC.Interfaces.API.Dirac                        import Dirac
from DIRAC.DataManagementSystem.Client.DataManager                 import DataManager
from BESDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from BESDIRAC.ResourceStatusSystem.SAM.TestExecutor                import TestExecutor
from BESDIRAC.ResourceStatusSystem.SAM.StatusEvaluator             import StatusEvaluator


__RCSID__ = '$Id:  $'
AGENT_NAME = 'ResourceStatus/SAMTestAgent'


class SAMTestAgent(AgentModule):
  """ SAMTestAgent
  
    The SAMTestAgent is used to execute SAM tests and evaluate SAM status
    periodically. It executes tests with TestExecutor and evaluates status with
    StatusEvaluator.
  """
    
  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
        
    self.tests = {}
    self.apis = {}
         
         
  def initialize(self):
    """ 
      specify the tests which need to be executed.
    """
    
    self.tests[ 'WMS-Test' ] = { 'module' : 'WMSTest', 'args' : { 'executable' : [ '/usr/bin/python', 'wms_test.py' ], 'timeout' : 1800 } }
    self.tests[ 'CVMFS-Test' ] = { 'module' : 'CVMFSTest', 'args' : { 'executable' : [ '/usr/bin/python', 'cvmfs_test.py' ], 'timeout' : 1800 } }
    self.tests[ 'BOSS-Test' ] = { 'module' : 'BOSSTest', 'args' : { 'executable' : [ '/usr/bin/python', 'boss_test.py' ], 'timeout' : 1800 } }
    self.tests[ 'SE-Test' ] = { 'module' : 'SETest', 'args' : { 'timeout' : 60 } }
    
    self.apis[ 'Dirac' ] = Dirac()
    self.apis[ 'DataManager' ] = DataManager()
    self.apis[ 'ResourceManagementClient' ] = ResourceManagementClient()
    
    return S_OK()
        
        
  def execute(self):
    """ 
      The main method of the agent. It get elements which need to be tested and 
      evaluated from CS. Then it instantiates TestExecutor and StatusEvaluate and 
      calls their main method to finish all the work.
    """
    
#     sites = [ { 'SiteName' : 'CLUSTER.CHENJ.cn',
#                'SiteType' : 'CLUSTER',
#                'ComputingElement' : [ 'chenj01.ihep.ac.cn' ],
#                'StorageElement' : [ 'IHEPD-USER' ] } ]
#         
#     ces = CSHelpers.getComputingElements()
#     ses = CSHelpers.getStorageElements()
#     sites = CSHelpers.getDomainSites()
#     if not sites[ 'OK' ]:
#       gLogger.error(sites[ 'Message' ])
#       return sites
    sites = { 'CLUSTER' : [ 'CLUSTER.CHENJ.cn' ], 'GRID' : [], 'CLOUD' : [] }
    ces = [ 'chenj01.ihep.ac.cn' ]
    ses = [ 'IHEPD-USER' ]
    clouds = sites[ 'CLOUD' ]
    elements = { 'ComputingElement' : ces, 'StorageElement' : ses, 'CLOUD' : clouds }
    
    executor = TestExecutor(self.tests, elements)
    testRes = executor.execute()
    if not testRes[ 'OK' ]:
      gLogger.error(testRes[ 'Message' ])
      return testRes
    testRes = testRes[ 'Value' ]
    
    evaluator = StatusEvaluator(sites)
    result = evaluator.evaluate(testRes)
    if not result[ 'OK' ]:
      gLogger.error(result[ 'Message' ])
      return result
            
    return S_OK()