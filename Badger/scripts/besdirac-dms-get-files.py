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
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27257_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27359_file0004.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27359_file0008.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27383_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27395_file0008.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27556_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27601_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27679_file0001.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27704_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run27870_file0005.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28086_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28087_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28110_file0001.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28118_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28119_file0007.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28129_file0002.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28144_file0004.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28153_file0007.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream009/664_jpsi_inc2_stream009_run28236_file0001.rtraw",
   ]
start = time.time()
badger.downloadFilesByDatasetName(fileList)
total = time.time()-start
exitCode=0
DIRAC.exit(exitCode)
  
