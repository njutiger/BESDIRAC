import json, copy
from datetime import datetime, timedelta
from DIRAC import S_OK
from WebAppDIRAC.Lib.WebHandler import WebHandler, WErr, WOK, asyncGen
from DIRAC.Core.DISET.RPCClient import RPCClient



class SAMHistoryHandler(WebHandler):
    
    AUTH_PROPS = "authenticated"
        
    @asyncGen
    def web_getSelectionData(self):
        callback = {}
        
        publisher = RPCClient('ResourceStatus/Publisher')
        
        sites = yield self.threadTask(publisher.getSites)
        if sites[ 'OK' ]:
            sites = [ [ site ] for site in sites[ 'Value' ] ]
        else:
            sites = [ [ 'Error happened on service side' ] ]
        
        types = yield self.threadTask(publisher.getDomains)
        if types[ 'OK' ]:
            types = [ [ type ] for type in types[ 'Value' ] ]
        else:
            types = [ [ 'Error happened on service side' ] ]
        
        mask = [ [ 'Active' ], [ 'Banned' ] ]
        
        vos = yield self.threadTask(publisher.getVOs)
        if vos[ 'OK' ]:
            vos = [ [ vo ] for vo in vos[ 'Value' ] ]
        else:
            vos = [ [ 'Error happened on service side' ] ]
        
        callback[ 'site' ] = sites
        callback[ 'type' ] = types
        callback[ 'mask' ] = mask
        callback[ 'vo' ] = vos
        
        self.finish(callback)
        

    @asyncGen        
    def web_getMainData(self):
        publisher = RPCClient('ResourceStatus/Publisher')
        
        sitesDict = {}
        
        sites = yield self.threadTask(publisher.getSites)
        if not sites[ 'OK' ]:
            self.finish({ 'success' : 'false', 'result' : [], 'total' : 0, 'error' : sites[ 'Message' ] })
            return
        sites = sites[ 'Value' ]
        
        sitesType = self.__getSitesType(sites)
        
        sitesMask = yield self.threadTask(self.__getSitesMaskStatus, sites)
        if not sitesMask[ 'OK' ]:
            self.finish({ 'success' : 'false', 'result' : [], 'total' : 0, 'error' : sitesMask[ 'Message' ] })
            return
        sitesMask = sitesMask[ 'Value' ]
        
        sitesVO = yield self.threadTask(self.__getSitesVO, sites)
        if not sitesVO[ 'OK' ]:
            self.finish({ 'success' : 'false', 'result' : [], 'total' : 0, 'error' : sitesVO[ 'Message' ] })
            return
        sitesVO = sitesVO[ 'Value' ]
        
        for site in sites:
            sitesDict[ site ] = {}
            sitesDict[ site ][ 'SiteType' ] = sitesType[ site ]
            sitesDict[ site ][ 'MaskStatus' ] = sitesMask[ site ]
            sitesDict[ site ][ 'VO' ] = sitesVO[ site ]
            
        req = self._request()
        
        selectedSites = req.get('Site')
        selectedType = req.get('SiteType')
        selectedMask = req.get('MaskStatus')
        selectedVOs = req.get('VO')
            
        sitesDict = yield self.threadTask(self.__siteFilte, sitesDict, selectedSites, selectedType, selectedMask, selectedVOs)
        
        if not sitesDict:
            self.finish({ 'success' : 'false', 'result' : [], 'total' : 0, 'error' : 'There are no data to display' })
            return
        
        if req.has_key('ToDate'):
            toDate = datetime.strptime(req[ 'ToDate' ], '%Y-%m-%d %H:%M')
        else:
            toDate = datetime.utcnow().replace(microsecond = 0)
            
        if req.has_key('FromDate'):
            fromDate = datetime.strptime(req[ 'FromDate' ], '%Y-%m-%d %H:%M')
        else:
            fromDate = toDate - timedelta(days = 1)
            
        samHistory = yield self.threadTask(publisher.getSitesSAMHistoryWeb, sitesDict.keys(), fromDate, toDate)
        if not samHistory[ 'OK' ]:
            self.finish({ 'success' : 'false', 'result' : [], 'total' : 0, 'error' : samHistory[ 'Message' ] })
            return
        total = samHistory[ 'Total' ]
        timestamps = samHistory[ 'Timestamps' ]
        samHistory = samHistory[ 'Value' ]
        
        if total == 0:
            self.finish({ 'success' : 'false', 'result' : [], 'total' : 0, 'error' : 'There is no data to display.' })
            return
        
        data = []
        dataFields =  [ { 'name' : 'Site' } ]
        columns = [ { 'text'  : 'Site', 'dataIndex' : 'Site' } ]
        
        for i in range(total):
            dataFields.append({ 'name' : str(i) })
            columns.append({ 'text' : timestamps[ i ], 'dataIndex' : str(i) })
        
        for siteName, statusList in samHistory.items():
            historyDict = { 'Site' : siteName }
            for i in range(total):
                if i >= len(statusList):
                    historyDict[ str(i) ] = '-'
                else:
                    historyDict[ str(i) ] = statusList[ i ]
            data.append(historyDict)
        
        self.finish({ 'success' : 'true', 'data' : data, 'dataFields' : dataFields, 'columns' : columns, 'total' : len(data) })
        
        
    def __siteFilte( self, sitesDict, selectedSites = None, selectedType = None, selectedMask = None, selectedVOs = None ):
        for siteName, siteDict in sitesDict.items():
            
            if selectedSites is not None and siteName not in selectedSites:
                del sitesDict[ siteName ]
                continue
            
            siteType = siteDict[ 'SiteType' ]
            if selectedType is not None and siteType not in selectedType:
                del sitesDict[ siteName ]
                continue
            
            siteMask = siteDict[ 'MaskStatus' ]
            if selectedMask is not None and siteMask not in selectedMask:
                del sitesDict[ siteName ]
                continue
            
            siteVO = copy.copy( siteDict[ 'VO' ] )
            if selectedVOs is not None and siteVO:
                for vo in siteVO:
                    if vo not in selectedVOs:
                        siteVO.remove( vo )
                        
                if not siteVO:
                    del sitesDict[ siteName ]
            
        return sitesDict
    
    
    def __getSitesType( self, sitesName ):
        sitesType = {}
        
        for siteName in sitesName:
            sitesType[ siteName ] = siteName.split( '.' )[ 0 ]
            
        return sitesType
        
    
    def __getSitesMaskStatus( self, sitesName ):
        wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
        
        activeSites = wmsAdmin.getSiteMask()
        
        if not activeSites[ 'OK' ]:
            return activeSites
        activeSites = activeSites[ 'Value' ]
        
        sitesStatus = {}
        
        for siteName in sitesName:
            if siteName in activeSites:
                sitesStatus[ siteName ] = 'Active'
            else:
                sitesStatus[ siteName ] = 'Banned'
                
        return S_OK( sitesStatus )
    
    
    def __getSitesVO( self, sitesName ):
        publisher = RPCClient( 'ResourceStatus/Publisher' )
        
        sitesVO = {}
        
        for siteName in sitesName:
            vo = publisher.getSiteVO( siteName )
            if not vo[ 'OK' ]:
                return vo
            sitesVO[ siteName ] = vo[ 'Value' ]
                
        return S_OK( sitesVO )
    
    
    def _request(self):
        req = {}
        
        if 'site' in self.request.arguments:
            site = list(json.loads(self.request.arguments[ 'site' ][ -1 ]))
            if len(site) > 0:
                req[ 'Site' ] = site
         
        if 'type' in self.request.arguments:
            type = list(json.loads(self.request.arguments[ 'type' ][ -1 ]))
            if len(type) > 0:
                req[ 'SiteType' ] = type
                
        if 'mask' in self.request.arguments:
            status = list(json.loads(self.request.arguments[ 'mask' ][ -1 ]))
            if len(status) > 0:
                req[ 'MaskStatus' ] = status
                       
        if 'vo' in self.request.arguments:
            vo = list(json.loads(self.request.arguments[ 'vo' ][ -1 ]))
            if len(vo) > 0:
                req[ 'VO' ] = vo
        
        if 'startDate' in self.request.arguments and  len( self.request.arguments["startDate"][0] ) > 0:
            if 'startTime' in self.request.arguments and len( self.request.arguments["startTime"][0] ) > 0:
                req["FromDate"] = str( self.request.arguments["startDate"][0] + " " + self.request.arguments["startTime"][0] )
            else:
                req["FromDate"] = str( self.request.arguments["startDate"][0] ) + " " + "00:00"
                
        if 'endDate' in self.request.arguments and  len( self.request.arguments["endDate"][0] ) > 0:
            if 'endTime' in self.request.arguments and len( self.request.arguments["endTime"][0] ) > 0:
                req["ToDate"] = str( self.request.arguments["endDate"][0] + " " + self.request.arguments["endTime"][0] )
            else:
                req["ToDate"] = str( self.request.arguments["endDate"][0] ) + " " + "00:00"
                
        return req

