import os

class LocalMount(object):
  def __init__(self):
    super(LocalMount, self).__init__()
    self.__mountPoint = '/dcache/bes'
    self._directlyRead = True

  def _available(self):
    return os.path.isdir(self.__mountPoint)

  def _retrieveRemoteAttribute(self, remotePath):
    attribute = {}
    if os.path.isfile(remotePath):
      attribute['size'] = os.path.getsize(remotePath)
      attribute['time'] = os.path.getmtime(remotePath)
    return attribute

  def _lfnToRemote(self, lfn):
    return self.__mountPoint + lfn
