#!/usr/bin/env python
#-*- coding:utf-8 -*-
#data:13/08/01

import os
from DIRAC.Interfaces.API.Dirac import Dirac
import DIRAC
from DIRAC import gLogger
from DIRAC import S_OK,S_ERROR
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

def addAndRegister(attributes,dir):

    guid = None
    localfile = attributes["PFN"]
    lfn=dir+"/"+attributes["LFN"]
    se = 'IHEP-USER' 
    storageElement = StorageElement(se)
    res = storageElement.isValid()
    if not res['OK']:
        errStr = "addAndRegister: The SE is not currently valid."
        return S_ERROR( errStr )
    destinationSE = storageElement.getStorageElementName()['Value']
    res = storageElement.getPfnForLfn( lfn )
    if not res['OK']:
        errStr = "addAndRegister: Failed to generate destination PFN."
        return S_ERROR( errStr )
    destPfn = res['Value']
    #print "pfn is %s"%destPfn
    #print "lfn is %s"%lfn

    #reset the LFN and PFN ,as metadata to register
    attributes["LFN"] = lfn
    attributes["PFN"] = destPfn

    #rm = ReplicaManager()
    dirac = Dirac()

    if not os.path.exists(localfile):
        errStr  = "File %s must exist locally"%localfile
        gLogger.error(errStr)
        return S_ERROR(errStr)
    if not os.path.isfile(localfile):
        errStr  = "File %s is not a file"%localfile
        gLogger.error(errStr)
        return S_ERROR(errStr)

    #res = rm.putAndRegister(lfn,localfile,se,guid,catalog=[])
    res = dirac.addFile(lfn,localfile,se,guid)
    if not res['OK']:
        errStr = "Error:filed to upload %s to %s"%(localfile,se)
        gLogger.error(errStr)
        return S_ERROR(errStr)
if __name__=="__main__":
    attributes = {"PFN":"dst.txt","LFN":"dst2.txt"}
    dir = "/bes/user/z/zhanggang"
    addAndRegister(attributes,dir)
    
