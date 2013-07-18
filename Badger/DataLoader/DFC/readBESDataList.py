#!/usr/bin/env python 
# -*- coding:utf-8 -*-
#data:13/07/18
##############################################################
#Function:
#    Read content from BES3DataList
#    Insert information of each line to /new_dfc_test/DataList
#    Add new entris to /dfc_test/new_test/DataList,just add new line to BES3DataList 
#    follow the order:
#        round runFrom runTo resonance dateFrom dateTo 
##############################################################


from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC import S_OK, S_ERROR

def dataListDir(client,items):
    #items is a list,with the order 'round runFrom runTo resonance
    #dateFrom dateTo'
    keys = ['round','runFrom','runTo','resonance','dateFrom','dateTo']
    
    items[1] = int(items[1])
    items[2] = int(items[2])
    #set entryName,items[3] is resonance,items[0] is roundId.
    #entryName = '/zg_test2/test1/DataList' + '/' + items[3] + '_' + items[0]
    entryName = '/dfc_test/new_test/DataList' + '/' + items[3] + '_' + items[0]
    result  = client.createDirectory(entryName)
    if result['OK'] and result['Value']['Successful'].has_key(entryName):
        i =0
        while i<len(keys):
            #print keys[i],items[i]
            client.setMetadata(entryName,{keys[i]:items[i]})
            i +=1
    else:
        return R_ERROR("Failed to create entry:%s"%entryName)

def main(client):
    with open('BES3DataList','r') as f:
        for line in f:
            items = line.strip().split()
            dataListDir(client,items)

if __name__ == '__main__':
    fcType = 'DataManagement/FileCatalogTest'
    client = FileCatalogClient(fcType)
    main(client)
