#!/usr/bin/env python

class GetFile:
  def __init__(self):
    self._directlyRead = False

  def getRemoteAttributes(self, lfnDict, useChecksum):
    raise Exception('not implemented')

  def get(self, filelist, dir):
    raise Exception('not implemented')

  def convertLfn(self, lfn):
    return lfn

  def directlyRead(self):
    return self._directlyRead
