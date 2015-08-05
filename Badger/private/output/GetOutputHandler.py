#!/usr/bin/env python

import os
import datetime

import pprint

from subprocess import call

class GetOutputHandler():
  def __init__(self, method, lfnList, outputDir, useChecksum, mergeFile, mergeSize, removeDownload):
    self.__method = method
    self.__lfnDict = {lfn: {} for lfn in lfnList}
    self.__outputDir = outputDir
    self.__useChecksum = useChecksum
    self.__mergeFile = mergeFile
    self.__mergeSize = mergeSize
    self.__removeDownload = removeDownload

    self.__createGetFile()

  def run(self):
    print 'There are %s files in the request' % len(self.__lfnDict)

    self.__preparePath()

    self.__prepareRemote()

    print 'There are %s files ready to be downloaded' % self.__remoteFileNumber()

    if self.__mergeFile and self.__getFile.directlyRead() and self.__removeDownload:
      self.__mergeFromRemote(self.__mergeSize)
    else:
      downloadOk = self.__download()
      if self.__mergeFile:
        self.__mergeFromLocal(self.__mergeSize)
        if downloadOk and self.__removeDownload:
          self.__removeLocalDownloaded()

  def __createGetFile(self):
    getFileClassName = ''.join(w.capitalize() for w in self.__method.split('_')) + 'GetFile'

    m = __import__('BESDIRAC.Badger.private.output.%s' % getFileClassName,globals(),locals(),[getFileClassName])
    self.__getFile = getattr(m, getFileClassName)()

  def __preparePath(self):
    pathDict = {}
    for lfn in self.__lfnDict:
      pathDict[lfn] = {'remote_path': self.__getFile.convertLfn(lfn),
                       'local_path': os.path.join(self.__outputDir, os.path.basename(lfn)),
                       'file_ext': os.path.splitext(lfn)[1]}
    self.__updateDict(pathDict)
#    pprint.pprint(self.__lfnDict)

  def __prepareRemote(self):
    remoteAttributes = self.__getFile.getRemoteAttributes(self.__lfnDict, self.__useChecksum)
    self.__updateDict(remoteAttributes)
#    pprint.pprint(self.__lfnDict)

  def __remoteFileNumber(self):
    counter = 0
    for lfn, value in self.__lfnDict.items():
      if 'remote_size' in value:
        counter += 1
    return counter

  def __updateDict(self, newDict):
    for lfn in newDict:
      if lfn in self.__lfnDict:
        self.__lfnDict[lfn].update(newDict[lfn])
      else:
        self.__lfnDict[lfn] = newDict[lfn]

  def __download(self):
    counter = 0
    for lfn in self.__lfnDict:
#      self.__getLocalAttributes(lfn)
#      if not self.__localValid(lfn):
#        self.__removeLocal(lfn)
      result = self.__getFile.get(self.__lfnDict[lfn])
      if result:
        print 'Downloaded OK: %s' % lfn
        counter += 1
#      self.__setFileTime(lfn)
      else:
        print 'Downloaded failed: %s' % lfn
    return counter


  def __getLocalAttributes(self, lfn):
    if lfn not in self.__lfnDict:
      raise Exception('ERROR: Unknown lfn %s' % lfn)
    localPath = self.__lfnDict[lfn]['local_path']
    if not os.path.isfile(localPath):
      print 'hehehhehehhe'
      return
    mtime = time.ctime(os.path.getmtime(f))
    print mtime
#    pprint.pprint(self.__lfnDict)

  def __localValid(self, lfn):
    return False

  def __removeLocal(self, lfn):
    return True

  def __setFileTime(self, lfn):
    return True

  def __getMergeLists(self):
    mergeLists = []

    mergeList = []
    for lfn in self.__lfnDict:
      mergeList.append(lfn)
    mergeList.sort()

    mergeLists.append(mergeList)
    return mergeLists

  def __doMerge(self, count, ext, fileList):
    mergeFileName = 'merge_%04d%s'%(count,ext)
    ret = call(['hadd', os.path.join(self.__outputDir, mergeFileName)] + fileList)
    return ret == 0

  def __doMergeList(self, lfnList, mergeSize):
    count = 0
    tempSize = 0
    tempList = []
#    for lfn in lfnList:
    for i in range(len(lfnList)):
      lfn = lfnList[i]
      tempSize += self.__lfnDict[lfn]['remote_size']
      tempList.append(self.__lfnDict[lfn]['local_path'])
      if i == len(lfnList) - 1 or tempSize > mergeSize or tempSize + self.__lfnDict[lfnList[i+1]]['remote_size'] > mergeSize:
        count += 1
        self.__doMerge(count, self.__lfnDict[lfn]['file_ext'], tempList)
        tempSize = 0
        tempList = []

  def __mergeFromLocal(self, mergeSize):
    mergeLists = self.__getMergeLists()
    for mergeList in mergeLists:
      self.__doMergeList(mergeList, mergeSize)
    return True
