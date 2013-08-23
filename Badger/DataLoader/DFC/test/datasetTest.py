#!/user/bin/env python
#-*- coding:utf-8 -*-
#############################################################
#$HeadURL:$
#date:2013/08/10
#author:gang
#############################################################
"""test dataset"""
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC import S_OK,S_ERROR
import pprint
from BESDIRAC.Badger.API.Badger import Badger
from BESDIRAC.Badger.DataLoader.DFC.readAttributes import DataAll,Others
def getFilesByLocaldir():
  badger = Badger()
  result = badger.getFilesByLocaldir("/bes3fs/offline/data/663-1/4260/dst/121215")
  print result[:10]
def addMetaFields(client):
  result = client.addMetadataField("dir1","varchar(30)")
  pprint.pprint(result)
  print "*"*70
def removeMetaFields(client,fileNameList):
  for fileName in fileNameList:
    client.deleteMetadataField(fieldName)
  pprint.pprint(S_OK)
  print "*"*70
  
def getAllMetaFileds(client):
  result = client.getMetadataFields()
  pprint.pprint(result)
  print "*"*70
def registerDataset(client):
  dataset_name = "dataset1"
  setDict = {"dir1":"bes"}
  result = client.addMetadataSet(dataset_name,setDict)
  pprint.pprint(result)
  if not result["OK"]:
    return S_ERROR("not OK")
  else:
    return S_OK()

  print "*"*70

def getFileByDatasetName(client):
  dataset_name = "dataset1"
  result = client.getMetadataSet(dataset_name,True)
  pprint.pprint(result)
  print "*"*70

#def listDataset(client):
#  result = client.listMetadatasets()
#  #result1 = client.listDirectory('/bes/user/')
#  pprint.pprint(result)
#  print "*"*70


if __name__=="__main__":
  from DIRAC.Core.Base import Script
  Script.parseCommandLine( ignoreErrors = True )
  client = FileCatalogClient()
  #addDirMetaFields(client)
  getAllMetaFileds(client)
  #listDataset(client)
  #registerDataset(client)
  #getFileByDatasetName(client)
  getFilesByLocaldir()
