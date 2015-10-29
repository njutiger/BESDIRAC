from subprocess import call

from DIRAC import gLogger

from BESDIRAC.Badger.private.output.getfile.GetFile import GetFile
from BESDIRAC.Badger.private.output.getfile.LocalMount import LocalMount

class CpGetFile(LocalMount, GetFile):
  def __init__(self):
    super(CpGetFile, self).__init__()

  def _downloadSingleFile(self, remotePath, localPath):
    gLogger.debug('cp from %s to %s' % (remotePath, localPath))
    ret = call(['cp', remotePath, localPath])
    return ret == 0
