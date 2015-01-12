#!/usr/bin/env python

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
List all the tasks

Usage:
   %s [option] ... [JobID] ...
""" % Script.scriptName )

Script.registerSwitch( "f:", "field=",          "Fields to list, seperated by comma. (TaskID,TaskName,Status,Owner,OwnerDN,OwnerGroup,Site,JobGroup,CreationTime)" )
Script.registerSwitch( "l:", "limit=",          "Task number limit" )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()
options = Script.getPositionalArgs()

from DIRAC.Core.DISET.RPCClient                      import RPCClient

def main():
  fields = ['TaskID','TaskName','Status','Owner','OwnerDN','OwnerGroup','Site','JobGroup','CreationTime']
  limit = 0
  for arg in args:
    (switch, val) = arg
    if switch == 'f' or switch == 'field':
      fields = val.split(',')
    if switch == 'l' or switch == 'limit':
      limit = int(val)

  taskClient = RPCClient('WorkloadManagement/TaskManager')
  result = taskClient.getTasks(fields, {}, limit, 'TaskID:DESC')
  if not result['OK']:
    print 'Get task list error: %s' % result['Message']
    return

  for task in result['Value']:
    print task

if __name__ == '__main__':
  main()
