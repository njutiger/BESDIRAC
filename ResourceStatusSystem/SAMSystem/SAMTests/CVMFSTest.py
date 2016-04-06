from DIRAC.ResourceStatusSystem.SAMSystem.SAMTests.BaseTest import BaseTest


__RCSID__ = '$Id: $'


class CVMFSTest( BaseTest ):
    testType = 'CVMFS-Test'
    elementType = [ 'ComputingElement', 'CLOUD' ]
    
    @staticmethod
    def _judge( log ):
        if log.find( 'Command error' ) != -1:
            return 'Bad'
        else:
            return 'OK'