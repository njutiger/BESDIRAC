import os
import time
import datetime

from DIRAC import gLogger
from DIRAC.Interfaces.API.Dirac import Dirac

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from BESDIRAC.Badger.private.output.getfile.GetFile import GetFile

class DfcGetFile(GetFile):
  def __init__(self):
    super(DfcGetFile, self).__init__()

    self.__fc = FileCatalogClient('DataManagement/FileCatalog')

  def _available(self):
    return True

  def _retrieveRemoteAttribute(self, remotePath):
    result = self.__fc.getFileMetadata([remotePath])
    if not result['OK']:
      raise Exception('getFileMetadata failed: %s' % result['Message'])

    if remotePath not in result['Value']['Successful']:
      return {}

    return self.__parseMetadata(result['Value']['Successful'][remotePath])

  def _retrieveAllRemoteAttributes(self, remotePathList):
    result = self.__fc.getFileMetadata(remotePathList)
    if not result['OK']:
      raise Exception('getFileMetadata failed: %s' % result['Message'])

    attributes = []
    for remotePath in remotePathList:
      attribute = {}
      if remotePath not in result['Value']['Successful']:
        attribute.append({})
      else:
        attribute.append(self.__parseMetadata(result['Value']['Successful'][remotePath]))

    return attributes

  def _downloadSingleFile(self, remotePath, localPath):
    gLogger.debug('getfile from %s to %s' % (remotePath, localPath))
    dirac = Dirac()
    result = dirac.getFile(remotePath, os.path.dirname(localPath))
    return result['OK']

  def __parseMetadata(self, metadata):
    attribute = {}
    attribute['size'] = metadata.get('Size', 0)
    attribute['time'] = self.__utc2Local(metadata.get('ModificationDate', datetime.datetime(1900,1,1,0,0,0)))
    if self._useChecksum:
      attribute[lfn]['checksum'] = metadata.get('Checksum', '')
      attribute[lfn]['checksum_type'] = metadata.get('ChecksumType', '')
    return attribute

  def __utc2Local(self, utc_st):
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local_st = utc_st + offset
    return time.mktime(local_st.utctimetuple())
