#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.initialize()
from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient


"""This is the public API for BADGER, the BESIII Advanced Data ManaGER.

   BADGER wraps the DIRAC File Catalog and related DIRAC methods for 
   use in the BESIII distributed computing environment.


"""

class Badger:

    def __init__(self, fcClient = False):
        """Internal initialization of Badger API.
        """       
        if not fcClient:
            _fcType = 'DataManagement/FileCatalog'
            self.client = FileCatalogClient(_fcType)
        else:
            self.client = fcClient

    
    def __registerDir(self,dir):
        """Internal function to register a new directory in DFC .
           Returns True for success, False for failure.
        """
        fc = self.client
        result = fc.createDirectory(dir)
        if result['OK']:
            if result['Value']['Successful']:
                if result['Value']['Successful'].has_key(dir):
                    return True
                elif result['Value']['Failed']:
                    if result['Value']['Failed'].has_key(dir):
                        print 'Failed to create directory %s:%s'%(dir,result['Value']['Failed'][dir])
                        return False
        else:
            print 'Failed to create directory %s:%s'%(dir,result['Message'])
            return False
        
        



    def __registerDirMetadata(self,dir,metaDict):
        """Internal function to set metadata to a directory
           Returns True for success, False for failure.
        """
        fc = self.client
        result = fc.setMetadata(dir,metaDict)
        if result['OK']:
            return True
        else:
            print ("Error for setting metadata %s to %s: %s" %(metaDict,dir,result['Message']))
            return False
        
    def __dirExists(self,dir,parentDir):
        """ Internal function to check whether 'dir' is the subdirectory of 'parentDir'
            Returns 1 for Yes, 0 for NO
        """
        fc = self.client
        dir_exists = 0
        result = fc.listDirectory(parentDir)
        if result['OK']:
            for i,v in enumerate(result['Value']['Successful'][parentDir]['SubDirs']):
                if v == dir: 
                    dir_exists = 1
                    break
        else:
            print 'Failed to list subdirectories of %s:%s'%(parentDir,result['Message'])
        
        return dir_exists

    def __registerSubDirs(self,dirs_dict,dirs_meta):
        """Internal function to create directories in dirs_dict
           Returns True for sucess, False for failure
        """
        creation_ok = True
        
        for dir in dirs_dict:
            if (dir != 'dir_file')&(dir !='dir_data_mc' ):
                if self.__registerDir(dirs_meta[dir][0]):
                    result = self.__registerDirMetadata(dirs_meta[dir][0],{dir.split('_')[1]:dirs_meta[dir][1]})
                    if not result:
                        creation_ok = False
                        break
                else:
                    print 'Failed to create %s'%dir
                    creation_ok = False
                    break
            else:
                if not self.__registerDir(dirs_meta[dir]):
                    print 'Failed to create %s'%dir
                    creation_ok = False
                    break

        return creation_ok
            
    def registerHierarchicalDir(self,metaDict,rootDir='/bes'):
        """
           Create a hierarchical directory according the metadata dictionary
           Return created directory  for sucess,if this directory has been created, return this existing directory .

           Structure of the hierarchical directory:
           for real data:/bes/File/resonance/boss version/data/eventType/expNum
           for mc data:/bes/File/resonance/boss version/mc/eventType/expNum/streamId
           The eventType of all real datas is all. 

           Example:
           >>>metaDic = {'dataType': 'dst', 'eventType': 'all', 'streamId': 'stream0','resonance': 'psipp', 'expNum':'exp1','bossVer': '6.6.1'}
           >>>badger.registerHierarchicalDir(metaDic)
           1
        """
        #Save about 20 lines compared with last one
        fc = self.client
        
        dir_exists = 0
        #0 for failure,1 for success,2 for existing directory
        creation_OK = 0
        lastDirMetaDict = {'dataType':metaDict['dataType'],'streamId':metaDict['streamId']}

        dir_file = rootDir + '/File'
        dir_resonance = dir_file + '/' + metaDict['resonance']
        dir_bossVer = dir_resonance + '/' + metaDict['bossVer']

        if metaDict['streamId'] == 'stream0':
            dir_data_mc = dir_bossVer + '/data'
        else:
            dir_data_mc = dir_bossVer + '/mc'
        dir_eventType = dir_data_mc + '/' +metaDict['eventType']
        dir_expNum = dir_eventType + '/' + metaDict['expNum']
        dir_streamId = dir_expNum + '/' + metaDict['streamId']

        # if dir_expNum has been created,create_expNum=1 
        create_expNum = 0

        dirs_dict = ['dir_file','dir_resonance','dir_bossVer','dir_data_mc','dir_eventType','dir_expNum']
        dirs_meta = {'dir_file':dir_file,'dir_data_mc':dir_data_mc,'dir_resonance':[dir_resonance,metaDict['resonance']],'dir_bossVer':[dir_bossVer,metaDict['bossVer']],'dir_eventType':[dir_eventType,metaDict['eventType']],'dir_expNum':[dir_expNum,metaDict['expNum']]}

        dir_exists = self.__dirExists(dir_file,rootDir)
        if not dir_exists:
            result = self.__registerSubDirs(dirs_dict,dirs_meta)
            if result:
                create_expNum = 1
        else:
            dir_exists = self.__dirExists(dir_resonance,dir_file)
            if not dir_exists:
                dirs_dict = dirs_dict[1:]
                result = self.__registerSubDirs(dirs_dict,dirs_meta)
                if result:
                    create_expNum = 1
            else:
                dir_exists = self.__dirExists(dir_bossVer,dir_resonance)
                if not dir_exists:
                    dirs_dict = dirs_dict[2:]
                    result = self.__registerSubDirs(dirs_dict,dirs_meta)
                    if result:
                        create_expNum = 1
                else:
                    dir_exists = self.__dirExists(dir_data_mc,dir_bossVer)
                    if not dir_exists:
                        dirs_dict = dirs_dict[3:]
                        result = self.__registerSubDirs(dirs_dict,dirs_meta)
                        if result:
                            create_expNum = 1
                    else:
                        dir_exists = self.__dirExists(dir_eventType,dir_data_mc)
                        if not dir_exists:
                            dirs_dict = dirs_dict[4:]
                            result = self.__registerSubDirs(dirs_dict,dirs_meta)
                            if result:
                                create_expNum = 1
                        else:
                            dir_exists = self.__dirExists(dir_expNum,dir_eventType)
                            if not dir_exists:
                                dirs_dict = dirs_dict[5:]
                                result = self.__registerSubDirs(dirs_dict,dirs_meta)
                                if result:
                                    create_expNum = 1
                            else:
                                create_expNum = 1
        
        if create_expNum:
            if metaDict['streamId'] != "stream0":
                dir_exists = self.__dirExists(dir_streamId,dir_expNum)
                if not dir_exists:
                    if self.__registerDir(dir_streamId):
                        result = self.__registerDirMetadata(dir_streamId,{'streamId':metaDict['streamId']})
                        if result:
                            result = self.__registerDirMetadata(dir_streamId,lastDirMetaDict)
                            if result:
                                creation_OK = 1
                else:
                    creation_OK = 2
            else:
                result = self.__registerDirMetadata(dir_expNum,lastDirMetaDict)
                if result:
                    creation_OK = 1
    
        if (creation_OK==1)|(creation_OK==2):
            if metaDict['streamId'] == "stream0":
                return dir_expNum
            else:   
                return dir_streamId

    def registerFileMetadata(self,lfn,metaDict):
        """Add file level metadata to an entry
           True for success, False for failure

           Example:
           >>>lfn = '/bes/File/psipp/6.6.1/data/all/exp1/run_0011414_All_file001_SFO-1'
           >>>entryDict = {'runL':1000,'runH':898898}
           >>>badger.registerFileMetadata(lfn,entryDict)
           True
        """
        fc = self.client
        result = fc.setMetadata(lfn,metaDict)
        if result['OK']:
            #print 'Successfully added file level metadatas to %s'%(lfn)
            return True
        else:
            print 'Error:%s'%(result['Message'])
            return False
        
  
    def registerFile(self,lfn,dfcAttrDict):
        """Register a new file in the DFC.
        
        """
        #TODO:need more tests,if directory of file doesn't exist,
        #addFile will create it without setting any metadata(lin lei)
        #need to check whether directory of file exists in dfc?(lin lei) 
        #pass
        fc = self.client
        result = fc.addFile({lfn:dfcAttrDict})
        if result['OK']:
            if result['Value']['Successful']:
                if result['Value']['Successful'].has_key(lfn):
                    return True
            elif result['Value']['Failed']:
                if result['Value']['Failed'].has_key(lfn):
                    print 'Failed to add this file:',result['Value']['Failed'][lfn]
                    return False
        else:
            print 'Failed to add this file :',result['Message']
            return False
        # need to register file (inc. creating appropriate directory
        # if it doesn't already exist; and register metadata for that
        # file / directory
        # Q: how / where to pass the metadata?
    

    def registerDataset(self, dataset_name, conditions):
        """Register a new dataset in DFC. Takes dataset name and string with
           conditions for new dataset as arguments.
        """
        pass
        # need to think about how datasets are defined
        # format for passing the dataset conditions?
        
        fc = self.client
        setDict = {}
        for cond in conditions:
            key, value = cond.split('=')
            setDict[key] = value
        result = fc.addMetadataSet(dataset_name, setDict)
        if not result['OK']:
            print ("Error: %s" % result['Message'])
        else:
            print "Added dataset %s with conditions %s" % (dataset_name, conditions)
        

    def getFilesByDatasetName(self, dataset_name):
        """Return a list of LFNs in the given dataset.
           
           Example usage:
           >>> badger.getFilesByDatasetName('psipp_661_data_all_exp2')
           ['/bes/File/psipp/6.6.1/data/all/exp2/file1', .....]
        """
        #TODO: checking of output, error catching

        fc = self.client
        result = fc.getMetadataSet(dataset_name, True)
        if result['Value']:
            metadataDict = result['Value']
            lfns = fc.findFilesByMetadata(metadataDict,'/')['Value']
            lfns.sort()
            return lfns
        else:
            print "ERROR: Dataset", dataset_name," not found"
            return None


    def getFilesByMetadataQuery(self, query):
        """Return a list of LFNs satisfying given query conditions.

           Example usage:
           >>> badger.getFilesByMetadataQuery('resonance=jpsi bossVer=6.5.5 expNum=exp1')
           ['/bes/File/jpsi/6.5.5/data/all/exp1/file1', .....]

        """
        #TODO: checking of output, error catching


        fc = self.client
        #TODO: calling the FileCatalog CLI object and its private method
        # is not a good way of doing this! but use it to allow construction of
        # the query meantime, until createQuery is made a public method
        cli = FileCatalogClientCLI(fc)
        metadataDict = cli._FileCatalogClientCLI__createQuery(query)
        result = fc.findFilesByMetadata(metadataDict,'/')
        if result['OK']:
            lfns = fc.findFilesByMetadata(metadataDict,'/')['Value']
            lfns.sort()
            return lfns
        else:
            print "ERROR: No files found which match query conditions."
            return None
  

    def getDatasetDescription(self, dataset_name):
        """Return a string containing a description of metadata with which 
           the given dataset was defined.
           
           Example usage:
           >>> result = badger.getDatasetDescription('psipp_661_data_all_exp2')
           >>> print result
           Dataset psipp_661_data_all_exp2 was defined with the following metadata conditions:
               expNum : exp2
               bossVer : 6.6.1
               resonance : psipp
        """
        #TODO: keep this as separate method, or just return description with LFNs?
        fc = self.client
        result = fc.getMetadataSet(dataset_name, True)
        if result['Value']:
            metadataDict = result['Value']
            # give user a reminder of what this dataset's definition is
            dataset_desc = ''
            dataset_desc += \
                'Dataset %s was defined with the following metadata conditions:\n' \
                % dataset_name
            for key in metadataDict:
                dataset_desc += '%s : %s\n' % (key, metadataDict[key])
        else:
            dataset_desc = 'Error: dataset %s is not defined.' % dataset_name
        return dataset_desc


    def listDatasets():
        pass


    def checkDatasetIntegrity():
        pass


