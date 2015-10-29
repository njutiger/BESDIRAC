from subprocess import call

from DIRAC import gLogger

class Rsync(object):
  def __init__(self):
    super(Rsync, self).__init__()

    # rsync could validate data by itself
#    self._localValidation = False

  def _downloadSingleFile(self, remotePath, localPath):
    gLogger.debug('rsync from %s to %s' % (remotePath, localPath))
    ret = call(['rsync', '-z', remotePath, localPath])
    return ret == 0
