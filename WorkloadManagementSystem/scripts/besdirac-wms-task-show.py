#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Show task detailed info

Usage:
   %s [option] ... [TaskID] ...
""" % Script.scriptName )

#Script.registerSwitch( "p",  "progress",        "Show task progress" )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()
options = Script.getPositionalArgs()

from DIRAC.Core.DISET.RPCClient                      import RPCClient
taskClient = RPCClient('WorkloadManagement/TaskManager')

def showTask(taskID):
  outFields = ['TaskID','TaskName','Status','Owner','OwnerDN','OwnerGroup','CreationTime','JobGroup','Site','Progress','Info']
  result = taskClient.getTask(taskID, outFields)
  if not result['OK']:
    print 'Get task error: %s' % result['Message']
    return

  for k,v in zip(outFields, result['Value']):
    if k in ['Progress', 'Info']:
      print '\n== %s ==' % k
      for kp,vp in json.loads(v).items():
        print '%s : %s' % (kp, vp)
    else:
      print '%s : %s' % (k, v)

  result = taskClient.getTaskJobs(taskID)
  if not result['OK']:
    print 'Get task error: %s' % result['Message']
    return
  print '\n== Jobs =='
  for jobID in result['Value']:
    print jobID,

  print '\n'

def main():
  for taskID in options:
    taskID = int(taskID)
    showTask(taskID)

if __name__ == '__main__':
  main()
