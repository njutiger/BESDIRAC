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
jobClient = RPCClient('WorkloadManagement/JobManager')

def deleteTask(taskID):
  result = taskClient.getTaskJobs(taskID)
  if not result['OK']:
    print 'Get task jobs error: %s' % result['Message']
    return
  jobIDs = result['Value']

  result = jobClient.deleteJob(jobIDs)
  if not result['OK']:
    print 'Delete jobs error: %s' % result['Message']
    return
#  print '%s job(s) deleted' % len(result['Value'])

  result = taskClient.removeTask(taskID)
  if not result['OK']:
    print 'Remove task error: %s' % result['Message']
    return
  print 'Task %s removed' % taskID

def main():
  for taskID in options:
    taskID = int(taskID)
    deleteTask(taskID)
    print ''

if __name__ == '__main__':
  main()
