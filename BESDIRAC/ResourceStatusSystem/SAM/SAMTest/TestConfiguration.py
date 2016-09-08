
TESTS = {

  'Access-Test' :
  {
    'module' : 'CEAccessTest',
    'match' : { 'ElementType' : ( 'ComputingElement', 'CLOUD' ) },
    'args' : { 'timeout' : 5 }
  },

  'WMS-Test' :
  {
    'module' : 'WMSTest',
    'match' : { 'ElementType' : ( 'ComputingElement', 'CLOUD' ) },
    'args' : { 'executable' : ( '/usr/bin/python', 'wms_test.py' ), 'timeout' : 3000 }
  },

  'CVMFS-Test' :
  {
    'module' : 'CVMFSTest',
    'match' : { 'ElementType' : ( 'ComputingElement', 'CLOUD' ), 'VO' : 'bes'  },
    'args' : { 'executable' : ( '/usr/bin/python', 'cvmfs_test.py' ), 'timeout' : 3000 }
  },

  'BOSS-Test' :
  {
    'module' : 'BOSSTest',
    'match' : { 'ElementType' : ( 'ComputingElement', 'CLOUD' ), 'VO' : 'bes' },
    'args' : { 'executable' : ( '/usr/bin/python', 'boss_test.py' ), 'timeout' : 3000 }
  },

  'CEPC-Test' :
  {
    'module' : 'CEPCTest',
    'match' : { 'ElementType' : ( 'ComputingElement', 'CLOUD' ), 'VO' : 'cepc' },
    'args' : { 'executable' : ( '/usr/bin/python', 'cepc_test.py' ), 'timeout' : 3000 }
   },

  'JUNO-Test':
  {
   'module' : 'JUNOTest',
   'match' : { 'ElementType' : ( 'ComputingElement', 'CLOUD' ), 'VO' : 'juno' },
   'args' : { 'executable' : ( '/usr/bin/python', 'juno_test.py' ), 'timeout' : 3000 }
   },

  'SE-Test' :
  {
    'module' : 'SETest',
    'match' : { 'ElementType' : ( 'StorageElement', ) }
  }

}