#!/usr/bin/env python

from BESDIRAC.Badger.private.output.RsyncGetFile import RsyncGetFile

class LocalRsyncGetFile(RsyncGetFile):
  def __init__(self):
    RsyncGetFile.__init__(self)
    self._directlyRead = True

  def get(self, src, dest):
    print 'Local ...'

  def convertLfn(self, lfn):
    return lfn
