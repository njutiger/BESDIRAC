#!/usr/bin/env python

import datetime

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from BESDIRAC.Badger.private.output.DirectGetFile import DirectGetFile

class DfcGetFile(DirectGetFile):
  def __init__(self):
    DirectGetFile.__init__(self)

    self.__fc = FileCatalogClient('DataManagement/FileCatalog')

  def getRemoteAttributes(self, lfnDict, useChecksum=False):
    result = self.__fc.getFileMetadata(lfnDict.keys())
    if not result['OK']:
      raise Exception('getFileMetadata failed: %s' % result['Message'])

    attrDict = {}
    for lfn, value in result['Value']['Successful'].items():
      attrDict[lfn] = {}
      attrDict[lfn]['remote_size'] = value.get('Size', 0)
      attrDict[lfn]['remote_time'] = value.get('ModificationDate', datetime.datetime(1900,1,1,0,0,0))
      if useChecksum:
        attrDict[lfn]['remote_checksum'] = value.get('Checksum', '')
        attrDict[lfn]['remote_checksum_type'] = value.get('ChecksumType', '')

    return attrDict

  def get(self, value):
    pass
