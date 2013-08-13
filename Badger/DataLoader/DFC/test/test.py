#!/usr/bin/env python
# -*- coding:utf-8 -*-
#data:13/07/18 

###############################################################################
# Function:                                                                   #
#         Read content of ExpSearch.txt                                       #
#         Insert information of each line ExpSearch.txt to /BES3/ExpSearch    #
#         If you want to add new entris to /BES3/ExpSearch, please add new    #
#         lines to a txt file according this order:                           #
#         runFrm,runTo,dateFrm,dateTo,expNum,resonance,roundId                #  
#                                                                             #
###############################################################################


import string
import uuid


from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC import S_OK,S_ERROR
home = '/zg_test1'
dir1 = home + '/' + 'dir1'
dir2 = dir1 + '/' + 'dir2'
dir3 = dir2 + '/' + 'dir3'

def listDir(client,dir):
    result= client.listDirectory(dir)

    for i,v in enumerate(result['Value']['Successful'][dir]['SubDirs']):
        result = client.getDirectoryMetadata(v)['Value']
        #runfrm = string.atoi(result['runFrm'])
        #runto = string.atoi(result['runTo'])
        #print runfrm,runto                                    
        print result

def removeDir(client,dir):
    result = client.removeDirectory(dir)
    if result['OK'] and result['Value']['Successful'].has_key(dir):
        return S_OK() 
    
def createTestDir(client):
    result1 = client.createDirectory(dir1)
    result2 = client.createDirectory(dir2)
    result3 = client.createDirectory(dir3)
    result4 = client.setMetadata(dir1,{"dir1_key":"dir1_metadata"})
    result5 = client.setMetadata(dir2,{"dir2_key":"dir2_metadata"})
    result6 = client.setMetadata(dir3,{"dir3_key":"dir3_metadata"})
    #
    #print result1
    #print result2
    #print result3
    #print result4
    #print result5
    #print result6
def addMetadataToFile(client):
    print client
    lfnList = []
    lfn1 = dir1+'/'+'t_LFN11'
    lfnList.append(lfn1)
    lfn2 = dir2+'/'+'t_LFN21'
    lfnList.append(lfn2)
    lfn3 = dir3+'/'+'t_LFN31'
    lfn3_2 = dir3+'/'+'t_LFN31'
    lfnList.append(lfn3)
    #lfn = '/bes/File/4260/6.6.3.p01/data/all/round06/run_0029677_All_file001_SFO-2'
    lfn = '/zhanggang_test/test/test0'
    dict = {'status': -1, 'description': 'null', 'dataType': 'dst', 'LFN': '/bes/File/4260/6.6.3.p01/data/all/round06/run_0029683_All_file001_SFO-2', 'PFN': 'srm://ccsrm.ihep.ac.cn/dpm/ihep.ac.cn/home/bes/bes/File/4260/6.6.3.p01/data/all/round06/run_0029683_All_file001_SFO-2', 'eventNum': 128466, 'fileSize': 386304986, 'date': '2013-08-13 19:05:19', 'runH': 29683, 'runL': 29683}

    
    #result = client.addFile(lfnList)
    #result41 = client.setMetadata(lfn1,{'file1_key':'file1_metadata'})
    #result32 = client.addFile({lfn3_2:{'PFN':'somePath','SE':'bes','Size':10,'Checksum':''}})
    result33 = client.setMetadata(lfn,dict)
    #print result
    #print result32
    print result33
def t_getMetadata(client,dir):
    result = client.listDirectory(dir)
    print result
    if result['OK']:
        for subDir in result['Value']['Successful'][dir]['SubDirs']:
            result = client.getDirectoryMetadata(subDir)['Value']
            print result
if __name__ == "__main__":
    ##fcType = 'DataManagement/FileCatalogTest'
    client = FileCatalogClient()
    ##dir = '/zhanggang_test/File/4260/6.6.3/data/all/round06/run_0029677_All_file001_SFO-1'
    addMetadataToFile(client)
    #dir = '/bes/user/'
    ##t_getMetadata(client,dir)
    ##client.createDirectory('/zhanggang_test/test1111')
    ##listDir(client,dir)
    #result = client.listDirectory(dir)
    #import pprint
    #pprint.pprint(result)
    ##client.setMetadata(dir,{'test1':1,'test2':2,'test3':3})
