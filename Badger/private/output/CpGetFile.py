#!/usr/bin/env python

import os
from subprocess import call

from BESDIRAC.Badger.private.output.DirectGetFile import DirectGetFile

class CpGetFile(DirectGetFile):
  def __init__(self):
    DirectGetFile.__init__(self)
#    self._directlyRead = True

  def getRemoteAttributes(self, lfnDict, useChecksum=False):
    attrDict = {}
    for lfn, value in lfnDict.items():
      attrDict[lfn] = {}
      if os.path.isfile(value['remote_path']):
        attrDict[lfn]['remote_size'] = os.path.getsize(value['remote_path'])
    return attrDict

  def get(self, value):
    if 'remote_path' not in value:
      raise Exception('remote_path not found')
    if 'local_path' not in value:
      raise Exception('local_path not found')
    source = value['remote_path']
    dest = value['local_path']
    ret = call(['cp', source, dest])
    return ret == 0

  def convertLfn(self, lfn):
    return '/dcache/bes' + lfn
