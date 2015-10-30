import time
import subprocess

from DIRAC import gConfig, gLogger

from BESDIRAC.Badger.private.output.getfile.GetFile import GetFile
from BESDIRAC.Badger.private.output.getfile.Rsync   import Rsync

class DaemonRsyncGetFile(Rsync, GetFile):
  def __init__(self):
    super(DaemonRsyncGetFile, self).__init__()

    self.__rsyncUrl = gConfig.getValue('/Resources/Applications/DataLocation/RsyncEndpoints/Url', 'rsync://localhost/bes-srm')
    gLogger.debug('Rsync daemon url:', self.__rsyncUrl)

  def _available(self):
    args = ['rsync', self.__rsyncUrl]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    ret = p.returncode

    return ret == 0 and out

  def _retrieveRemoteAttribute(self, remotePath):
    attribute = {}

    args = ['rsync', remotePath]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    ret = p.returncode

    if ret == 0 and out:
      attribute['size'] = int(out.split()[1].replace(',', ''))
      time_str = '%s %s' % (out.split()[2], out.split()[3])
      attribute['time'] = time.mktime(time.strptime(time_str, '%Y/%m/%d %H:%M:%S'))

    return attribute

  def _lfnToRemote(self, lfn):
    return self.__rsyncUrl + lfn
