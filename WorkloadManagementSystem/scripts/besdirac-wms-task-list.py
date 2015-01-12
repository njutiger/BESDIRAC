#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
List all the tasks

Usage:
   %s [option] ... [JobID] ...
""" % Script.scriptName )

Script.registerSwitch( "f:", "field=",          "Fields to list, seperated by comma. (TaskID,TaskName,Status,Owner,OwnerDN,OwnerGroup,CreationTime,Site,JobGroup)" )
Script.registerSwitch( "l:", "limit=",          "Task number limit" )
Script.registerSwitch( "p",  "progress",        "Show task progress" )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()
options = Script.getPositionalArgs()

from DIRAC.Core.DISET.RPCClient                      import RPCClient

fieldFormat = {
  'TaskID'      : ('%-8s',  '%-8s',  8),
  'TaskName'    : ('%-16s', '%-16s', 16),
  'Status'      : ('%-12s', '%-12s', 12),
  'Owner'       : ('%-12s', '%-12s', 12),
  'OwnerGroup'  : ('%-12s', '%-12s', 12),
  'CreationTime': ('%-20s', '%-20s', 20),
  'Site'        : ('%-16s', '%-16s', 16),
  'JobGroup'    : ('%-16s', '%-16s', 16),
}

def showTitle(fields):
  for field in fields:
    if field == 'Progress':
      print '%-8s' % 'Total',
      print '%-8s' % 'Done',
      print '%-8s' % 'Failed',
      print '%-8s' % 'Running',
      print '%-8s' % 'Waiting',
      print '%-8s' % 'Deleted',
    else:
      print fieldFormat[field][0] % field,
  print ''

  for field in fields:
    if field == 'Progress':
      print '-'*8,
      print '-'*8,
      print '-'*8,
      print '-'*8,
      print '-'*8,
      print '-'*8,
    else:
      print '-'*fieldFormat[field][2],
  print ''

def showTask(fields, task):
  for field, value in zip(fields, task):
    if field == 'Progress':
      prog = json.loads(value)
      print '%8s' % prog.get('Total', 0),
      print '%8s' % prog.get('Done', 0),
      print '%8s' % prog.get('Failed', 0),
      print '%8s' % prog.get('Running', 0),
      print '%8s' % prog.get('Waiting', 0),
      print '%8s' % prog.get('Deleted', 0),
    else:
      print fieldFormat[field][1] % value,
  print ''

def main():
  fields = ['TaskID','TaskName','Status','Owner','OwnerGroup','CreationTime','Site','JobGroup']
  limit = 0
  addProgress = False
  for arg in args:
    (switch, val) = arg
    if switch == 'f' or switch == 'field':
      fields = val.split(',')
    if switch == 'l' or switch == 'limit':
      limit = int(val)
    if switch == 'p' or switch == 'progress':
      addProgress = True

  if addProgress and 'Progress' not in fields:
    fields.append('Progress')

  if limit == 0:
    orderAttribute = 'TaskID:ASC'
  else:
    orderAttribute = 'TaskID:DESC'

  taskClient = RPCClient('WorkloadManagement/TaskManager')
  result = taskClient.getTasks(fields, {}, limit, orderAttribute)
  if not result['OK']:
    print 'Get task list error: %s' % result['Message']
    return

  # Title
  showTitle(fields)

  # Task value
  for task in result['Value']:
    showTask(fields, task)

if __name__ == '__main__':
  main()
