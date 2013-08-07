#!/usr/bin/env python
# Lin Lei, Caitriana Nicholson
# (ported from Lin Lei's data loading tool for AMGA)

###############################################################################
# The function of this programme                                              #
# Create directories as follows:                                              #
# /BES3/File/Resonance/BossVersion/Data                                       #
# /BES3/File/Resonance/BossVersion/MC                                         #
# /BES3/SearchExp                                                             #
# /BES3/EventTypeList                                                         #
###############################################################################
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
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



def createFileDir(client, resonance, bossVer, type):
    dir = '/BES3/File'
    result = client.createDirectory(dir)
    if _dir_ok(result, dir):
        for r in resonance:
            res_dir = dir+"/"+r
            result = client.createDirectory(res_dir)
            client.setMetadata(res_dir,{'resonance':r})
            if not _dir_ok(result, res_dir):
                 return False
            for b in bossVer:
                 boss_dir = res_dir+"/"+b
                 result = client.createDirectory(boss_dir)
                 client.setMetadata(boss_dir, {'bossVer':b})
                 if not _dir_ok(result, boss_dir):
                     return False
                 for t in type:
                     type_dir = boss_dir + "/" + t
                     result = client.createDirectory(type_dir)
                     if not _dir_ok(result, type_dir):
                         return False
        return True  
    else:
        return False

def main(client):
    resonance=["jpsi","psip","psipp","psi4040","con3650","psippscan"]
    bossVer=["6.5.5","6.6.1"]
    type=["data","mc"]

    # create root directory
    dir = '/BES3'
    result = client.createDirectory(dir)
    if _dir_ok(result, dir):
        createFileDir(client, resonance, bossVer, type)
        try:
            client.addMetadataField('eventType','VARCHAR(30)')
            client.addMetadataField('expNum','VARCHAR(10)')
            client.addMetadataField('streamId','VARCHAR(10)')
            client.addMetadataField('dataType','VARCHAR(10)')
            client.addMetadataField('bossVer','VARCHAR(10)')
            client.addMetadataField('runL','int')
            client.addMetadataField('runH','int')
            client.addMetadataField('resonance','VARCHAR(30)')
            client.addMetadataField('status','int')
            client.addMetadataField('description','VARCHAR(100)')
        except ex:
            print "Error adding metadata fields: ", ex


        dir = '/BES3/ExpSearch'
        result = client.createDirectory(dir)
        if _dir_ok(result, dir):
            print "Created ExpSearch directory"
        dir = '/BES3/EventTypeList'
        result = client.createDirectory(dir)
        if _dir_ok(result, dir):
            print "Created EventTypeList directory"

if __name__ == "__main__":
    client = FileCatalogClient()
    main(client)

