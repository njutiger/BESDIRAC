#!/usr/bin/env python

from BESDIRAC.Badger.private.output.RsyncGetFile import RsyncGetFile

class DaemonRsyncGetFile(RsyncGetFile):
  def get(self, src, dest):
    print 'Daemon ...'

  def convertLfn(self, lfn):
    return lfn
