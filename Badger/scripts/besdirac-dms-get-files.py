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
   "/bes/File/jpsi/664/mc/inc2/round05/stream001/664_jpsi_inc2_stream001_run27684_file0007.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream001/664_jpsi_inc2_stream001_run27685_file0001.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream001/664_jpsi_inc2_stream001_run27745_file0003.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream001/664_jpsi_inc2_stream001_run28004_file0007.rtraw",
   "/bes/File/jpsi/664/mc/inc2/round05/stream001/664_jpsi_inc2_stream001_run28004_file0006.rtraw",
   ]
start = time.time()
badger.downloadFilesByDatasetName(fileList)
total = time.time()-start
exitCode=0
DIRAC.exit(exitCode)
  
