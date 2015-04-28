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
jobmonClient = RPCClient('WorkloadManagement/JobMonitoring')

def rescheduleTask(taskID, status=[]):
  result = taskClient.getTaskJobs(taskID)
  if not result['OK']:
    print 'Get task jobs error: %s' % result['Message']
    return
  jobIDs = result['Value']
  print jobIDs

  if status:
    result = jobmonClient.getJobs( { 'JobID': jobIDs, 'Status': status } )
    if not result['OK']:
      print 'Get jobs of status %s error: %s' % (status, result['Message'])
      return
    jobIDs = result['Value']
    print jobIDs

  result = jobClient.rescheduleJob(jobIDs)
  if not result['OK']:
    print 'Reschedule jobs error: %s' % result['Message']
    return
  print '%s job(s) rescheduled' % len(result['Value'])

def main():
  for taskID in options:
    taskID = int(taskID)
    rescheduleTask(taskID, ['Failed'])
    print ''

if __name__ == '__main__':
  main()
