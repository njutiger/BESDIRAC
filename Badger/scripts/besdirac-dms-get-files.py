#!/usr/bin/env python
#############################################
#$HeadURL:$
#data:2013/08/07
#author:gang
#############################################
"""
download a list of files from SE to the current directory
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
Script.registerSwitch("m","datasetName","the dataset you want to download")
Script.setUsageMessage('\n'.join([__doc__,
                                'Usage:',
                                '%s dir'% Script.scriptName,
                                'Arguments:'
                                ' datasetName: the dataset you want to download']))
Script.parseCommandLine(ignoreErrors=True)

import time
from DIRAC.Interfaces.API.Dirac import Dirac
from BESDIRAC.Badger.API.Badger import Badger
badger = Badger()
fileList=[
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27263_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27349_file0008.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27383_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27435_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27436_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27443_file0004.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27615_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27616_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27650_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27667_file0001.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27668_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27704_file0006.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27720_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27728_file0001.rtraw ",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27732_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27750_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27880_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27915_file0004.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27964_file0004.rtraw ",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run27971_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28003_file0001.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28033_file0004.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28036_file0006.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28037_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28038_file0001.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28086_file0008.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28087_file0007.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28105_file0004.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28137_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28143_file0004.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28147_file0009.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28149_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28156_file0008.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28160_file0001.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28191_file0002.rtraw ",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28227_file0006.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28236_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream007/664_jpsi_inc2_stream007_run28236_file0003.rtraw",
   ]
start = time.time()
badger.downloadFilesByDatasetName(fileList)
total = time.time()-start
exitCode=0
DIRAC.exit(exitCode)
  
