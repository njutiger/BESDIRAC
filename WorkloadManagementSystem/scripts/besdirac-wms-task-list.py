#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
List all the tasks

Usage:
   %s [option]
""" % Script.scriptName )

Script.registerSwitch( "f:", "field=",          "Fields to list, seperated by comma. (Owner,OwnerDN,OwnerGroup,CreationTime,Progress,JobGroup,Site)" )
Script.registerSwitch( "w",  "whole",           "List all the fields" )
Script.registerSwitch( "l:", "limit=",          "Task number limit" )
Script.registerSwitch( "e",  "expired",         "Include expired tasks" )
Script.registerSwitch( "r",  "removed",         "Include removed tasks (only for admin)" )
Script.registerSwitch( "u",  "users",           "Include other users' tasks (only for admin)" )
Script.registerSwitch( "a",  "all",             "Show all tasks" )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()
options = Script.getPositionalArgs()

from DIRAC.Core.Security.ProxyInfo                   import getProxyInfo
from DIRAC.Core.DISET.RPCClient                      import RPCClient
taskClient = RPCClient('WorkloadManagement/TaskManager')

fieldFormat = {
  'TaskID'      : ('%-8s',  '%-8s',  8,  'TaskID'),
  'TaskName'    : ('%-24s', '%-24s', 24, 'TaskName'),
  'Status'      : ('%-12s', '%-12s', 12, 'Status'),
  'Owner'       : ('%-12s', '%-12s', 12, 'Owner'),
  'OwnerDN'     : ('%-12s', '%-12s', 12, 'OwnerDN'),
  'OwnerGroup'  : ('%-12s', '%-12s', 12, 'OwnerGroup'),
  'CreationTime': ('%-20s', '%-20s', 20, 'CreationTime'),
  'Progress'    : ('%-24s', '%-24s', 24, 'Progress(T/(D,F,R,W,O))'),
  'Site'        : ('%-16s', '%-16s', 16, 'Site'),
  'JobGroup'    : ('%-16s', '%-16s', 16, 'JobGroup'),
}

def showTitle(fields):
  for field in fields:
    print fieldFormat[field][0] % fieldFormat[field][3],
  print ''

  for field in fields:
    print '-'*fieldFormat[field][2],
  print ''

def showTask(fields, task):
  for field, value in zip(fields, task):
    if field == 'Progress':
      prog = json.loads(value)
      progStr = '%s/(%s,%s,%s,%s,%s)' \
        % (prog.get('Total', 0), prog.get('Done', 0), prog.get('Failed', 0),
           prog.get('Running', 0), prog.get('Waiting', 0), prog.get('Deleted', 0))
      print fieldFormat[field][1] % progStr,
    else:
      print fieldFormat[field][1] % value,
  print ''

def main():
  basicFields = ['TaskID','TaskName','Status']

  fields = ['Owner','OwnerGroup','CreationTime','Progress']
  limit = -1
  showExpired = False
  showOtherUser = False
  showRemoved = False
  for arg in args:
    (switch, val) = arg
    if switch == 'f' or switch == 'field':
      fields = val.split(',')
    if switch == 'w' or switch == 'whole':
      fields = ['Owner','OwnerGroup','CreationTime','Progress','JobGroup','Site']
    if switch == 'l' or switch == 'limit':
      limit = int(val)
    if switch == 'e' or switch == 'expired':
      showExpired = True
    if switch == 'r' or switch == 'removed':
      showRemoved = True
    if switch == 'u' or switch == 'user':
      showOtherUser = True
    if switch == 'a' or switch == 'all':
      showExpired = True
      showOtherUser = True
      showRemoved = True

  fields = basicFields + fields;

  condDict = {}
  condDict['Status'] = ['Init', 'Ready', 'Processing', 'Finished']
  if showExpired:
    condDict['Status'].append('Expired')
  if showRemoved:
    condDict['Status'].append('Removed')

  if not showOtherUser:
    result = getProxyInfo()
    if result['OK']:
      condDict['OwnerDN'] = result['Value']['identity']

  orderAttribute = 'TaskID:DESC'

  result = taskClient.getTasks(fields, condDict, limit, orderAttribute)
  if not result['OK']:
    print 'Get task list error: %s' % result['Message']
    return

  # Title
  showTitle(fields)

  # Task value
  taskList = list(result['Value'])
  taskList.reverse()
  for task in taskList:
    showTask(fields, task)

if __name__ == '__main__':
  main()
