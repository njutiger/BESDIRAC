#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
import re

#该文件用于删除某一目录下文件名符合一定正则表达式的文件
def removeAllFiles(client,dir):
    n = 0
    result = client.listDirectory(dir)
    #print result
    
    if result['OK']:
        #print result
        for i,v in enumerate(result['Value']['Successful'][dir]['Files']):
            all = re.compile(r"run_\d+_All_file\d+_SFO-\d+_[56789]")
            #all = re.compile(r"[1]$")
            flag_all = all.search(v)
            if flag_all is not None:
                print 'v',v
                n += 1
                result = client.removeFile(v)
                if not result['OK']:
                    print 'Failed to remove %s'%v  

    return n


if __name__=='__main__':
    fcType = 'DataManagement/FileCatalogTest'
    client = FileCatalogClient(fcType)
    
    #dir = '/BES3/File/psi4040/6.5.5/data/all/exp1'
    #dir = '/dfc_test/File/psipp/6.5.5/data/all/exp1'
    #dir = '/dfc_test/File/psi4040/6.6.1/data/all/exp1'
    #dir = '/dfc_test/File/psip/6.6.1/data/all/exp2'
    #dir = '/dfc_test/File/psip/6.6.1/data/all/exp2'
    dir = '/dfc_test/File/psipp/6.6.1/data/all/exp3'
    num = removeAllFiles(client,dir)
    print num

    

