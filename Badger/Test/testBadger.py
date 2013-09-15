#!/usr/bin/env python
import os
import time
import pprint
import DIRAC

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()
client = FileCatalogClient()
#condition1 = ['resonance=jpsi','bossVer=6.6.4','eventType=inclusive','dataType=dst','round=round02','streamId=stream001']
#condition = ['resonance=jpsi','bossVer=6.6.4','eventType=all','dataType=dst','round=round02','streamId=stream0']
#pprint.pprint(badger.registerDataset('jpsi_6.6.4_all_round02_stream0_dst',condition))
#pprint.pprint(badger.listDatasets())
#pprint.pprint(badger.getFilesByDatasetName('jpsi_6.6.4_all_round02_stream0_dst'))
#pprint.pprint(badger.getDatasetDescription('jpsi_6.6.4_all_round02_stream0_dst'))
#badger.removeDir("/zhanggang_test/File/jpsi/6.6.4")
#start = time.time()
#badger.uploadAndRegisterFiles('/besfs2/offline/data/664-1/jpsi/09mc/dst')
#totaltime=time.time()-start
#print totaltime
#badger.uploadAndRegisterFiles('/besfs2/offline/data/664-1/jpsi/dst')
#badger.testFunction()
#badger.removeDir('/bes/File/jpsi/6.6.4')


#con = ['dataType=rtraw','bossVer=664','resonance=jpsi','eventType=inc2','expNum=round05','streamId=stream004']
#pprint.pprint(badger.registerDataset('jpsi_664_inc2_round05_stream004_rtraw',con))
#pprint.pprint(badger.listDatasets())
pprint.pprint(badger.getFilesByDatasetName('jpsi_664_inc2_round05_stream003_rtraw'))
#result = badger.getFilesByDatasetName('jpsi_664_inc2_round05_stream001_rtraw')
#hfileList = [ os.path.basename(item) for item in result]
#print len(result)
#pprint.pprint(fileList)
#for item in fileList:
#  print item
#pprint.pprint(badger.getDatasetDescription('jpsi_6.6.4_all_round02_stream0_dst'))


