from BESDIRAC.Badger.private.output.RsyncGetFile import GetFile
from BESDIRAC.Badger.private.output.RsyncGetFile import Rsync

class DaemonRsyncGetFile(Rsync, GetFile):
  def get(self, src, dest):
    print 'Daemon ...'

  def convertLfn(self, lfn):
    return lfn
