#/usr/bin/env python
# -*- conding:utf-8 -*-
#data:13/08/01

#put the data to SE and register them inti DFC


from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors = True)
import os
#from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC import gLogger
import DIRAC

rm = ReplicaManager()


#from DIRAC.Resources.Storage.StorageElement import StorageElement
storageElement = StorageElement('IHEPD_USER')
