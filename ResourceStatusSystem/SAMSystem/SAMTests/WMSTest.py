from DIRAC.ResourceStatusSystem.SAMSystem.SAMTests.BaseTest import BaseTest


__RCSID__ = '$Id: $'


class WMSTest( BaseTest ):
    testType = 'WMS-Test'
    elementType = [ 'ComputingElement', 'CLOUD' ]
    
    @staticmethod
    def _judge( log ):
        if log.find( 'hello' ) != -1:
            return 'OK'
        else:
            return 'Bad'
