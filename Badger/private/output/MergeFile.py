import os

from subprocess import call

from DIRAC import gLogger

class MergeFile(object):
  def __init__(self):
    self._directlyRead = False
    self._localValidation = True

  def merge(self, fileList, outputDir, mergeName, mergeExt, mergeSize, mergeCallback):
    allFileSize = self.__getAllFileSize(fileList)

    count = 0
    tempSize = 0
    tempList = []
    for i in range(len(fileList)):
      fn = fileList[i]
      tempSize += allFileSize[fn]
      tempList.append(fn)
      if i == len(fileList) - 1 or tempSize > mergeSize or tempSize + allFileSize[fileList[i+1]] > mergeSize:
        count += 1
        mergePath = os.path.join(outputDir, '%s_%04d%s' % (mergeName, count, mergeExt))

        ret = self.__doMerge(tempList, mergePath)
        if mergeCallback is not None:
          mergeCallback(tempList, mergePath, tempSize, ret)
        if not ret:
          return False

        tempSize = 0
        tempList = []

    return True

  def __doMerge(self, fileList, mergePath):
    if len(fileList) == 0:
      gLogger.error('Can not merge empty file list!')
      return False

    try:
      ret = call(['hadd', mergePath] + fileList)
    except Exception, e:
      gLogger.error('Command "hadd" not found. Can not merge files. Please check your environment!')
      return False

    return ret == 0

  def __getAllFileSize(self, fileList):
    allFileSize = {}
    for fn in fileList:
      allFileSize[fn] = os.path.getsize(fn)
    return allFileSize
