#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

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

def insertToExp(client, items):
    #items is a list,and contains values of 
    #'runFrm','runTo','dateFrm','dateTo','expNum','resonance','roundId'
    
    keys=['runFrm','runTo','dateFrm','dateTo','expNum','resonance','roundId']
    
    #In items, the types of runFrm and runTo are str
    runfrm=string.atoi(items[0])
    runto=string.atoi(items[1])
    expnum = string.lower(items[4])
    resonance = string.lower(items[5])
    roundid = string.lower(items[6])
    
    values=[runfrm,runto,items[2],items[3],expnum,resonance,roundid]

    #set entry name is resonance+"_"+expnum
    entryName='/BES3/ExpSearch/'+resonance+"_"+expnum
    #client=FileCatalogClient()
    result = client.createDirectory(entryName)
    if result['OK'] and result['Value']['Successful'].has_key(entryName):
        i = 0
        while i < len(keys):
            client.setMetadata(entryName,{keys[i]:values[i]})
            i = i + 1
    else:
        print "Failed to create entry: "+entryName
        

def main(client):
    
    filename = raw_input('Enter file name:')
    f = open(filename,'r')
    allLines = f.readlines()
    f.close()

    for eachLine in allLines:
        data=eachLine.strip()
        items=data.split(',')
                
        insertToExp(client, items)

        
if __name__=='__main__':
    client = FileCatalogClient()
    main(client)
