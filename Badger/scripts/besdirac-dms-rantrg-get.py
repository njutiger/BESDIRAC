#!/usr/bin/env python

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Insert random trigger file into the File Catalog 

Usage:
   %s [option]
""" % Script.scriptName )

fcType = 'FileCatalog'
Script.registerSwitch( "f:", "file-catalog=", "Catalog client type to use (default %s)" % fcType )
Script.registerSwitch( "j:", "jopts=",  "jobOptions.txt" )
Script.registerSwitch( "r:", "runmin=", "Minimun run number" )
Script.registerSwitch( "R:", "runmax=", "Maximum run number" )
Script.registerSwitch( "e:", "se=",     "SE name" )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()


from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalogFactory          import FileCatalogFactory
from DIRAC.Core.Utilities.SiteSEMapping                  import getSEsForSite

import sys
import re
import socket

SeSiteMap = {
  'BES.JINR.ru'       : 'JINR-USER',
  'BES.IHEP-PBS.cn'   : 'IHEPD-USER',
  'BES.GUCAS.cn'      : 'IHEPD-USER',
  'BES.USTC.cn'       : 'USTC-USER',
  'BES.WHU.cn'        : 'WHU-USER',
}

SeDomainMap = {
  'jinrru'      : 'JINR-USER',
  'ihepaccn'    : 'IHEPD-USER',
  'ustceducn'   : 'USTC-USER',
  'whueducn'    : 'WHU-USER',
}



def determineSeFromSite():
    siteName = DIRAC.siteName()
    SEname = SeSiteMap.get(siteName, '')
    if not SEname:
        result = getSEsForSite(siteName)
        if result['OK'] and result['Value']:
            SEname = result['Value'][0]
    return SEname

def determineSeFromDomain():
    fqdn=socket.getfqdn()
    domain=''.join(fqdn.split('.')[-2:])
    if domain=='accn' or domain=='educn':
        domain=''.join(socket.getfqdn().split('.')[-3:])

    SEname = SeDomainMap.get(domain, '')
    return SEname

def determineSe():
    se = determineSeFromSite()
    if se:
        return se

    return determineSeFromDomain()

def getFile(lfn, se=''):
    rm = ReplicaManager()

    download_ok = 0
    get_active_replicas_ok = False
    lfn_on_se = False
    error_msg = ''
    for i in range(0, 16):
        result = rm.getActiveReplicas(lfn)
        if result['OK'] and result['Value']['Successful']:
            get_active_replicas_ok = True
            lfnReplicas = result['Value']['Successful']
            if se in lfnReplicas[lfn]:
                lfn_on_se = True
                break

    if not get_active_replicas_ok:
        return S_ERROR('Get replicas error: %s' % lfn)

    if lfn_on_se:
        pfn = lfnReplicas[lfn][se]
        # try 5 times
        for j in range(0, 5):
            result = rm.getStorageFile(pfn, se)
            if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(pfn):
                break
        if result['OK']:
            if result['Value']['Successful'] and result['Value']['Successful'].has_key(pfn):
                download_ok = 1
            else:
                error_msg = 'Downloading %s(%s) from SE %s error!' % (lfn, pfn, se)
        else:
            error_msg = result['Message']
    else:
        print >>sys.stderr, 'File %s not found on SE "%s" after %s tries, trying other SE' % (lfn, se, i+1)
        # try 5 times
        for j in range(0, 5):
            result = rm.getFile(lfn)
            if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
                break
        if result['OK']:
            if result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
                download_ok = 2
            else:
                error_msg = 'Downloading %s from random SE error!' % lfn
        else:
            error_msg = result['Message']

    if download_ok:
        return S_OK({lfn: {'DownloadOK': download_ok, 'Retry': j+1}})

    return S_ERROR(error_msg)

def parseOpt(filename):
    f = open(filename, 'r')
    fileContent = f.read()
    mat = re.findall('RealizationSvc\s*\.\s*RunIdList.*?;', fileContent, re.DOTALL)
    if not mat:
        return (0, 0)

    line = mat[-1]
    tmp = ''.join(line.split())
    line = tmp.replace('[', '{').replace(']', '}')

    vars = line.split('{')[1].split('}')[0].split(',')

    if len(vars) == 1:
        runmin = runmax = abs(int(vars[0]))
    elif len(vars) == 3 and int(vars[1]) == 0:
        runmin = abs(int(vars[0]))
        runmax = abs(int(vars[2]))
        if runmax < runmin:
            temp = runmax
            runmax = runmin
            runmin = temp
    else:
        runmin = runmax = 0

    return (runmin, runmax)

def findFiles(runnb):
    result = FileCatalogFactory().createCatalog(fcType)
    if not result['OK']:
        print >>sys.stderr, result['Message']

    catalog = result['Value']

    (runmin,runmax) = runnb[0]

    res = catalog.findFilesByMetadata({'runL':{'>=':runmin},'runH':{'<=':runmax}}, '/bes/File/randomtrg')
     
    lfns=[]
    for f in res['Value']:
       lfns.append(f)

    return lfns

def main():
    jfile = ''
    runmin = 0
    runmax = 0
    se = ''
    for arg in args:
        (switch, val) = arg
        if switch == 'j' or switch == 'jopts':
            jfile = val
        if switch == 'r' or switch == 'runmin':
            runmin = int(val)
        if switch == 'R' or switch == 'runmax':
            runmax = int(val)
        if switch == 'e' or switch == 'se':
            se = val

    if jfile != '':
        (runmin, runmax) = parseOpt(jfile)
    
    if (runmin, runmax) == (0, 0):
        print >>sys.stderr, 'No input run range. Check arguments or jobOptions.txt'
        sys.exit(65)

    if(runmax < runmin):
        temp = runmax
        runmax = runmin
        runmin = temp

    print "Run range:", runmin, runmax

    if not se:
        se = determineSe()
        print "Determine SE:", se

    lfns = findFiles([(runmin, runmax)])
    print '%s files found in run %s - %s' % (len(lfns), runmin, runmax)
    for lfn in lfns:
        result = getFile(lfn, se)
        print result
        if not result['OK']:
            print >>sys.stderr, 'Download file %s from SE "%s" error:' % (lfn, se)
            print >>sys.stderr, result
            sys.exit(66)


if __name__ == '__main__':
    main()
