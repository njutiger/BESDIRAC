#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
import re
import itertools
#该文件用于查询某一个目录下存储的文件个数

def getFilesNum(client,dir):
    n = 0
    result = client.listDirectory(dir)
    
    if result['OK']:
        for i,v in enumerate(result['Value']['Successful'][dir]['Files']):
            n += 1

    return n


if __name__=='__main__':
    fcType = 'DataManagement/FileCatalogTest'
    client = FileCatalogClient(fcType)
    resonace = ['con3650','psi4040','psipp','jpsi','psip','psippscan']
    bossVer = ['6.5.5','6.6.1']
    expNum = ['exp1','exp2','exp3']
    file = '/dfc_test/File/'
    dataAll = '/data/all/'
    
    l = itertools.product(resonace,bossVer,expNum)
    dict = {}
    for i,j,k in l:
        dir = file + i + '/' + j  + dataAll + k
        num = getFilesNum(client,dir)
        dict[dir] = num
    
    print '---------------------------------------'
    print '  Directory             file number    '
    print '---------------------------------------'
    for i in dict.keys():
        print '%s                        %s'%(i,str(dict[i]))

    

