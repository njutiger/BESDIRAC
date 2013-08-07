#!/usr/bin/env python
# Authors: Lin Lei, Caitriana Nicholson
# uses code from DFC too
# (ported from Lin Lei's code for inserting to AMGA)

import time
import re

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

def _dir_ok(result, dir):
    """Internal function to check success or failure of directory creation.
       Returns True for success, False for failure
    """
    if result['OK']:
        if result['Value']['Successful']:
            if result['Value']['Successful'].has_key(dir):
                #print "Successfully created directory:", dir
                return True
        elif result['Value']['Failed']:
            if result['Value']['Failed'].has_key(dir):
                print 'Failed to create directory:',result['Value']['Failed'][dir]
            return False
    else:
        print 'Failed to create directory:',result['Message']
        return False

def _set_metadata(client, attributes, dir):
    """Internal function to set metadata values on a given directory.
       Returns True for success, False for failure.
    """
    metadataDict = {}
    metadataDict['dataType'] = attributes['dataType']
    metadataDict['runL'] = attributes['runL']
    metadataDict['runH'] = attributes['runH']
    metadataDict['streamId'] = attributes['streamId']
    metadataDict['status'] = attributes['status']
    metadataDict['description'] = attributes['description']

    result = client.setMetadata(dir, metadataDict)
    if result['OK']:
        return True
    else:
        return False


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

def createCatalog(client, dir, eventType, expNum, attributes):
    #TODO: add error handling, try/catch blocks etc

    streamId = attributes['streamId']

    #check whether dir+eventType exists in DFC
    #list all eventTypes under "dir" catalog
    dir_exists = 0
    evt_type_dir = dir + "/" + eventType
    expnum_dir = evt_type_dir + "/" + expNum
    result = client.listDirectory(dir)
    if result['OK']:
        for i,v in enumerate(result['Value']['Successful'][dir]['SubDirs']):
            if v == evt_type_dir:
                dir_exists = 1
                break

    if not dir_exists:
        # create eventType and expNum directories
        result = client.createDirectory(evt_type_dir)
        if _dir_ok(result, evt_type_dir):
            #set metadata for the eventType dir
            result = client.setMetadata(evt_type_dir, {'eventType':eventType})
            if not result['OK']:
                print ("Error: %s" % result['Message'])

            result = client.createDirectory(expnum_dir)
            
            if _dir_ok(result, expnum_dir):
                result = client.setMetadata(expnum_dir, {'expNum':expNum})
                if not result['OK']:
                    print ("Error: %s" % result['Message'])
                if streamId == "stream0":
                    # real data, so this is lowest level directory - 
                    # add all remaining metadata
                    result = _set_metadata(client, attributes, expnum_dir)
                    if not result:
                        print ("Error setting metadata")

    else:
        # eventType directory exists, check for expNum subdir
        dir_exists = 0
        result = client.listDirectory(evt_type_dir)
        if result['OK']:
            for i,v in enumerate(result['Value']['Successful'][evt_type_dir]['SubDirs']):
                if v == expnum_dir:
                    dir_exists = 1
                    break

        if not dir_exists:
            # create expNum directory; if real data (not MC), add 
            # metadata attributes here
            result = client.createDirectory(expnum_dir)
            #TODO: set metadata for expNum dir
            if _dir_ok(result, expnum_dir):
                result = client.setMetadata(expnum_dir, {'expNum':expNum})
                if not result['OK']:
                    print ("Error: %s" % result['Message'])
                if streamId == "stream0":
                    result = _set_metadata(client, attributes, expnum_dir)
                    if not result:
                        print ("Error setting metadata")

    if streamId != "stream0":
        # MC data, check for stream subdirs
        exists = 0
        result = client.listDirectory(expnum_dir)    
        if result['OK']:
            for i,v in enumerate(result['Value']['Successful'][expnum_dir]['SubDirs']):
                if v == expnum_dir+"/"+streamId:
                    exists = 1
                    break
        
        if not exists:
            # create streamId subdir
            stream_dir = expnum_dir+"/"+streamId
            result = client.createDirectory(stream_dir)
            #TODO: add streamId metadata for stream dir
            if _dir_ok(result, stream_dir):
                result = _set_metadata(client, attributes, stream_dir) 
                if not result:
                    print ("Error setting metadata")

    if streamId == "stream0":
        return expnum_dir
    else:
        return expnum_dir+"/"+streamId


def insert(attributes):

    dir = "/BES3/File/"+attributes["resonance"]+"/"+attributes["bossVer"]

    #get real eventType and expNum in catalog in amga
    eventType = _get_event_type(attributes["eventType"])
    expNum = _get_exp_num(attributes["expNum"])

    if attributes["streamId"]=="stream0":
        dir=dir+"/data"
    else:
        dir=dir+"/mc"

    client = FileCatalogClient()
    
    # make sure insertion directory exists in DFC
    insertDir = createCatalog(client,dir,eventType,expNum,attributes)

    #do we need to explicitly add LFN attr in DFC?
    # we do need to get the PFN, size, se for addFile (below)
    #TODO pfn = ?
    # need to add PFN attribute to the list, probably in control.py
    pfn = attributes["PFN"]
    lfn=insertDir+"/"+attributes["LFN"]
    attributes["LFN"] = lfn
    #TODO se = ?  temporarily use a made-up SE name
    se = 'BES-TEST' 

    # register files in DFC
    infoDict = {}
    #infoDict['PFN'] = attributes['PFN']
    infoDict['PFN'] = pfn
    infoDict['Size'] = attributes['fileSize']
    infoDict['SE'] = se
   # infoDict['guid'] = attributes['guid']
    infoDict['Checksum'] = ''
    result = client.addFile({lfn:infoDict})
    print result
    #client.setMetadata    

