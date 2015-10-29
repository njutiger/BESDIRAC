import os
import time
import imp

from subprocess import call

from DIRAC import gLogger

from BESDIRAC.Badger.private.output.MergeFile import MergeFile

class GetOutputHandler(object):
  def __init__(self, lfnList, method, localValidation, useChecksum):
    self.__lfnList = lfnList

    if type(method) is list:
      self.__method = self.__decideBestMethod(method)
    else:
      self.__method = method

    self.__createGetFile()
    self.__getFile.setUseChecksum(useChecksum)
    self.__getFile.setLocalValidation(localValidation)

    self.__checkRemote()

    self.__mergeFile = MergeFile()

  def getMethod(self):
    return self.__method

  def getAvailNumber(self):
    return len(self.__lfnAvailList)

  def download(self, downloadDir, downloadCallback=None):
    count = {}

    for lfn in self.__lfnAvailList:
      result = self.__getFile.getFile(lfn, downloadDir)
      if result['status'] in count:
        count[result['status']] += 1
      else:
        count[result['status']] = 1

      if downloadCallback is not None:
        downloadCallback(lfn, result)

    return count

  def downloadAndMerge(self, downloadDir, mergeDir, mergeName, mergeExt, mergeSize, removeDownload,
                       downloadCallback=None, mergeCallback=None, removeCallback=None):
    if self.__getFile.directlyRead() and removeDownload:
      mergeNumber = self.__mergeFromRemote(mergeDir, mergeName, mergeExt, mergeSize, mergeCallback)
    else:
      count = self.download(downloadDir, downloadCallback)
      if mergeSize > 0:
        if 'error' not in count or count['error'] == 0:
          ret = self.__mergeFromLocal(downloadDir, mergeDir, mergeName, mergeExt, mergeSize, mergeCallback)
          if removeDownload and ret:
            self.__removeLocalDownloaded(downloadDir, removeCallback)


  def __checkRemote(self):
    allRemoteAttributes = self.__getFile.getAllRemoteAttributes(self.__lfnList)
    self.__lfnAvailList = [lfn for lfn in self.__lfnList if allRemoteAttributes[lfn]]

  def __createGetFile(self):
    getFileClassName = ''.join(w.capitalize() for w in self.__method.split('_')) + 'GetFile'
    getFileModuleName = 'BESDIRAC.Badger.private.output.getfile.%s' % getFileClassName

    try:
      self.__getFile = self.__loadClass(getFileModuleName, getFileClassName)
    except ImportError, e:
      raise Exception('Could not find method %s' % self.__method)

  def __decideBestMethod(self, methodList):
    for method in methodList:
      getFileClassName = ''.join(w.capitalize() for w in method.split('_')) + 'GetFile'
      getFileModuleName = 'BESDIRAC.Badger.private.output.getfile.%s' % getFileClassName

      try:
        getFile = self.__loadClass(getFileModuleName, getFileClassName)
      except ImportError, e:
        raise Exception('Could not find method %s' % self.__method)

      if getFile.available():
        return method

    return 'dfc'


  def __mergeFromLocal(self, downloadDir, mergeDir, mergeName, mergeExt, mergeSize, mergeCallback):
    mergeList = self.__lfnAvailList[:]
    localMergeList = [self.__getFile.lfnToLocal(downloadDir, lfn) for lfn in mergeList]
    localMergeList.sort()
    return self.__mergeFile.merge(localMergeList, mergeDir, mergeName, mergeExt, mergeSize, mergeCallback)

  def __mergeFromRemote(self, mergeDir, mergeName, mergeExt, mergeSize, mergeCallback):
    mergeList = self.__lfnAvailList[:]
    remoteMergeList = [self.__getFile.lfnToRemote(lfn) for lfn in mergeList]
    remoteMergeList.sort()
    return self.__mergeFile.merge(remoteMergeList, mergeDir, mergeName, mergeExt, mergeSize, mergeCallback)

  def __removeLocalDownloaded(self, downloadDir, removeCallback):
    for lfn in self.__lfnAvailList:
      localPath = self.__getFile.lfnToLocal(downloadDir, lfn)
      if os.path.isfile(localPath):
        os.remove(localPath)
        if removeCallback is not None:
          removeCallback(localPath)

  def __loadClass(self, moduleName, className):
    try:
      m = __import__(moduleName, globals(), locals(), [className])
    except ImportError, e:
      raise Exception('Could not from %s import %s' % (moduleName, className))
    return getattr(m, className)()
