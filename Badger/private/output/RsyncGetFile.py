#!/usr/bin/env python

from BESDIRAC.Badger.private.output.GetFile import GetFile

class RsyncGetFile(GetFile):
  def get(self, src, dest):
    print 'Rsync ...'

  def convertLfn(self, lfn):
    return lfn
