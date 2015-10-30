import os
import time

from DIRAC import gLogger

class GetFile(object):
  def __init__(self):
    super(GetFile, self).__init__()

    # Cache for remote attributes
    self._remoteAttributes = {}

    # These attributes could be reset in the derived classes
    self._directlyRead = False
    self._localValidation = True
    self._checksumType = 'Md5'

################################################################################
# These methods should be implemented in the derived classes

  def _available(self):
    raise Exception('not implemented')

  def _downloadSingleFile(self, remotePath, localPath):
    raise Exception('not implemented')

  def _retrieveRemoteAttribute(self, remotePath):
    ''' Get size, time and optional checksum
    '''
    raise Exception('not implemented')


# These methods could be implemented optionally

  def _lfnToRemote(self, lfn):
    return lfn

  def _retrieveAllRemoteAttributes(self, remotePathList):
    attributes = []
    for remotePath in remotePathList:
      attributes.append(self.__getRemoteAttribute(remotePath))
    return attributes


################################################################################
# Common methods for GetFile

  def available(self):
    return self._available()

  def lfnToRemote(self, lfn):
    return self._lfnToRemote(lfn)

  def lfnToLocal(self, dir, lfn):
    return self.__lfnToLocal(dir, lfn)

  def setUseChecksum(self, useChecksum = True):
    self._useChecksum = useChecksum

  def setLocalValidation(self, localValidation = True):
    self._localValidation = localValidation

  def directlyRead(self):
    return self._directlyRead

  def getAllRemoteAttributes(self, lfnList):
    remotePathList = [self._lfnToRemote(lfn) for lfn in lfnList]
    attributes = self._retrieveAllRemoteAttributes(remotePathList)
    return {lfn: attribute for lfn, attribute in zip(lfnList, attributes)}

  def getRemoteAttribute(self, lfn):
    remotePath = self._lfnToRemote(lfn)
    return self.__getRemoteAttribute(remotePath)

  def getFile(self, lfn, dir):
    return self.__getFile(lfn, dir)


################################################################################
# Private methods for GetFile

  def __lfnToLocal(self, dir, lfn):
    return os.path.join(dir, os.path.basename(lfn))

  def __getFile(self, lfn, dir):
    result = {}

    remotePath = self._lfnToRemote(lfn)
    localPath = os.path.join(dir, os.path.basename(lfn))

    remoteAttribute = self.getRemoteAttribute(lfn)
    if not remoteAttribute:
      gLogger.debug('Remote file does not exist:', remotePath)
      result['status'] = 'notexist'
      return result

    result['size'] = remoteAttribute['size']

    if self._localValidation:
      if self.__localValid(remotePath, localPath):
        gLogger.debug('Skip downloading %s. %s already exists' % (remotePath, localPath))
        result['status'] = 'skip'
        return result

    self.__removeLocal(localPath)

    startTime = time.time()
    ret = self._downloadSingleFile(remotePath, localPath)
    endTime = time.time()
    result['span'] = endTime - startTime

    if not ret:
      gLogger.debug('Download error: %s -> %s' % (remotePath, localPath))
      result['status'] = 'error'
      return result

    self.__setFileTime(localPath, remoteAttribute['time'])
    result['status'] = 'ok'

    return result


  def __getRemoteAttribute(self, remotePath):
    if remotePath in self._remoteAttributes:
      return self._remoteAttributes[remotePath]

    attribute = self._retrieveRemoteAttribute(remotePath)
    self._remoteAttributes[remotePath] = attribute
    return attribute

  def __getLocalAttribute(self, localPath):
    attribute = {}

    if not os.path.isfile(localPath):
      gLogger.debug('File not exists for getting attribute:', localPath)
      return attribute

    size = os.path.getsize(localPath)
    attribute['size'] = size

    mtime = os.path.getmtime(localPath)
    attribute['time'] = mtime

    return attribute


  def __localValid(self, remotePath, localPath):
    remoteAttribute = self.__getRemoteAttribute(remotePath)
    localAttribute = self.__getLocalAttribute(localPath)

    if not (remoteAttribute and localAttribute):
      return False

    if remoteAttribute['size'] != localAttribute['size']:
      return False

    if remoteAttribute['time'] != localAttribute['time']:
      return False

    return True

  def __setFileTime(self, localPath, mtime):
    os.utime(localPath, (mtime, mtime))

  def __removeLocal(self, localPath):
    if os.path.isfile(localPath):
      gLogger.debug('Remove invalid local file:', localPath)
      os.remove(localPath)
