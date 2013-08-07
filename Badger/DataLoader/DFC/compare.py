#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei
#from amga import mdclient,mdinterface

import re
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

def _get_event_type(eventType):
    #check whether userN exists in filename
    pat = re.compile(r'user\d+')
    des = pat.search(eventType)

    # userN exists in filename
    if des is not None:
        return "user"
    else:   
        return eventType

def _get_exp_num(expNum):
    #check whether "mexp" exists in filename
    pat = re.compile(r'mexp')
    des = pat.search(expNum)

    if des is not None:
        return "mexp"
    else:
        return expNum
        

def compare(attributes,input):
    error_list = {}
    inDFC = {}
    
    #print "input keys:",input.keys()
    keys = sorted(input.keys())
    #print "keys after being sorted:",keys
    
    expNum = _get_exp_num(attributes["expNum"])
    eventType = _get_event_type(attributes["eventType"])
    #dir = "/BES3/File/"+attributes["resonance"]+"/"+attributes["bossVer"]

    dir = "/BES3/File/"+attributes["resonance"]+"/"+attributes["bossVer"]

    if attributes["streamId"]=="stream0":
        dir = dir+ "/data"+"/"+eventType + "/"+expNum+"/"+attributes["LFN"]
    else:
        dir = dir+"/mc"+"/"+eventType+"/"+expNum+"/"+attributes["streamId"]+"/"+attributes["LFN"]

    client=FileCatalogClient()
    
    result = client.getFileMetadata(dir)

    file_exist = len(result['Value']['Successful'])
    if file_exist == 0:
        print "this file does't exist in DFC",attributes['LFN']
    else:    
        result = client.getDirectoryMetadata(dir)

        if result["OK"]:
            inDFC["resonance"] = result["Value"]["resonance"]
            inDFC["streamId"] = result["Value"]["streamId"]
            inDFC["eventType"] = result["Value"]["eventType"]
            inDFC["bossVer"] = result["Value"]["bossVer"]
            inDFC["expNum"] = result["Value"]["expNum"]
        
        for key in keys:
            if input[key] != inDFC[key]:
                error_list[key] = inDFC[key]


        if error_list is not None:
            return error_list

if __name__=="__main__":
    attribute = {}
    input = {}
    attribute["resonance"] = "psi4040"
    attribute["bossVer"] = "6.5.5"
    attribute["eventType"] = "all"
    attribute["expNum"] = "exp1"
    attribute["streamId"] = "stream0"
    attribute["LFN"] = "run_0023466_All_file001_SFO-2"

    input ["resonance"] = "psipp"
    input ["bossVer"] = "6.6.1"
    
    result = compare(attribute,input)
    print result
