#!/usr/bin/env python
import time
import pprint
import DIRAC
import os
from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()
client = FileCatalogClient()
#condition1 = ['resonance=jpsi','bossVer=6.6.4','eventType=inclusive','dataType=dst','round=round02','streamId=stream001']
condition = ['resonance=jpsi','bossVer=664','eventType=inc2','dataType=rtraw','expNum=round05','streamId=stream008']
pprint.pprint(badger.registerDataset('jpsi_664_inc2_round05_stream008_rtraw_1',condition))
#pprint.pprint(badger.listDatasets())
pprint.pprint(badger.getFilesByDatasetName('jpsi_664_inc2_round05_stream008_rtraw_1'))
#for lfn in badger.getFilesByDatasetName('jpsi_664_inc2_round05_stream007_rtraw'):
#  print os.path.basename(lfn) 
#pprint.pprint(badger.getDatasetDescription('jpsi_6.6.4_all_round02_stream0_dst'))
#badger.removeDir("/zhanggang_test/File/jpsi/6.6.4")
#start = time.time()
#badger.uploadAndRegisterFiles('/besfs2/offline/data/664-1/jpsi/09mc/dst')
#totaltime=time.time()-start
#print totaltime
#badger.uploadAndRegisterFiles('/besfs2/offline/data/664-1/jpsi/dst')
#badger.testFunction()
#badger.removeDir('/bes/File/jpsi/6.6.4')

