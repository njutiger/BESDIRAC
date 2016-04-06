''' SAMTestAgent

  This agent sends SAM tests to sites and evaluates test results.

'''

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.DISET.RPCClient                                      import RPCClient
from DIRAC.ResourceStatusSystem.SAMSystem.StatusEvaluator import StatusEvaluator


__RCSID__ = '$Id:  $'
AGENT_NAME = 'ResourceStatus/SAMTestAgent'


class SAMTestAgent(AgentModule):
    
    def __init__(self, *args, **kwargs):
        AgentModule.__init__(self, *args, **kwargs)
        
        self.tests = {}
        self.apis = {}
         
         
    def initialize(self):
        self.tests[ 'WMS-Test' ] = { 'module' : 'WMSTest', 'executable' : [ '/usr/bin/python', 'wms_test.py' ], 'timeout' : 900 }
        self.tests[ 'CVMFS-Test' ] = { 'module' : 'CVMFSTest', 'executable' : [ '/usr/bin/python', 'cvmfs_test.py' ], 'timeout' : 900 }
        
        
    def execute(self):
        sites = [ { 'SiteName' : 'CLUSTER.CHENJ.cn',
                   'SiteType' : 'CLUSTER',
                   'ComputingElement' : [ 'chenj01.ihep.ac.cn' ],
                   'StorageElement' : [ 'IHEPD-USER' ] } ]
        
        evaluator = StatusEvaluator(sites, self.tests)
        result = evaluator.evaluate()
        
        if not result[ 'OK' ]:
            gLogger.error(result[ 'Message' ])
            gLogger.error('SAMTest Agent execute failed.')
            
        return S_OK()
        
    
#     def __getSitesInfo(self):
#         
#         _basePath = 'Resources/Sites'
#         
#         sitesInfo = []
#         
#         domainNames = gConfig.getSections(_basePath)
#         if not domainNames[ 'OK' ]:
#             return domainNames
#         domainNames = domainNames[ 'Value' ]
#           
#         for domainName in domainNames:
#             domainSites = gConfig.getSections('%s/%s' % (_basePath, domainName))
#             if not domainSites[ 'OK' ]:
#                 return domainSites
#             domainSites = domainSites[ 'Value' ]
#               
#             for domainSite in domainSites:
#                 siteDict = {}
#                 
#                 siteDict[ 'Name' ] = domainSite
#                 siteDict[ 'Type' ] = domainName
#                 
#                 ses = gConfig.getValue('%s/%s/%s/SE' % (_basePath, domainName, domainSite), 'nouse')
#                 siteDict[ 'StorageElement' ] = ses
#                   
#                 ces = gConfig.getSections('%s/%s/%s/CEs' % (_basePath, domainName, domainSite))
#                 if not ces[ 'OK' ]:
#                     ces = []
#                 else:
#                     ces = ces[ 'Value' ]
#                 siteDict[ 'ComputingElement' ] = ','.join(ces) or 'nouse'
#                   
#                 vos = gConfig.getValue('%s/%s/%s/VO' % (_basePath, domainName, domainSite))
#                 if vos is None:
#                     
#                     for ce in ces:
#                         vos = gConfig.getValue('%s/%s/%s/CEs/%s/VO' % (_basePath, domainName, domainSite, ce))                        
#                         if vos is None:
#                             
#                             queues = gConfig.getSections('%s/%s/%s/CEs/%s/Queues' % (_basePath, domainName, domainSite, ce))
#                             if not queues[ 'OK' ]:
#                                 queues = []
#                             else:
#                                 queues = queues[ 'Value' ]
#                                   
#                             for queue in queues:
#                                 vos = gConfig.getValue('%s/%s/%s/CEs/%s/Queues/%s/VO' % (_basePath, domainName, domainSite, ce, queue))
#                                 if vos is not None:
#                                     break
#                             
#                         if vos is not None:
#                             break
#                         
#                 siteDict[ 'VO' ] = vos or 'novos'
#                 
#                 wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
#                 results = wmsAdmin.getSiteSummaryWeb({ 'Site' : domainSite }, [], 0, 0)
#                 if not results[ 'OK' ]:
#                     return results
#                 results = results[ 'Value' ]
#                 
#                 if not 'ParameterNames' in results:
#                     return S_ERROR('Wrong result dictionary, missing "ParameterNames"')
#                 params = results[ 'ParameterNames' ]
#     
#                 if not 'Records' in results:
#                     return S_ERROR('Wrong formed result dictionary, missing "Records"')
#                 records = results[ 'Records' ]
#                 
#                 siteDict[ 'MaskStatus' ] = dict(zip(params , records[ 0 ]))[ 'MaskStatus' ]
#                 
#                 sitesInfo.append(siteDict)
#                 
#         return sitesInfo
        
