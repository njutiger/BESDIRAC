#!/usr/bin/env python
# -*- coding:utf-8 -*-
#data:13/07/18
#improve mkdir command,if dir has file or subdir,the first remove file and subdir,then remove dir. 
#删除目录首先检查时候有子目录和file，如果有就删除，然后在删除次目录。因为removeDirectory()只能删除空目录。
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC import S_OK, S_ERROR

def removeDir(client,dir):
    result = client.listDirectory(dir)
    #print result
    if result['OK']:
        if not result['Value']['Successful'][dir]['Files'] and not result['Value']['Successful'][dir]['SubDirs']:
            print 'no file and subDirs in this dir'
            client.removeDirectory(dir)
            return S_OK()
        else:
            if result['Value']['Successful'][dir]['Files']:
                for file in result['Value']['Successful'][dir]['Files']:
                    client.removeFile(file)
            else:
                for subdir in result['Value']['Successful'][dir]['SubDirs']:
                    removeDir(client,subdir)
                removeDir(client,dir)
        




if __name__=="__main__":
    fcType = 'DataManagement/FileCatalogTest'
    client = FileCatalogClient(fcType)
    dir='/zg_test2'
    removeDir(client,dir)
