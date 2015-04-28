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

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()
options = Script.getPositionalArgs()

from DIRAC.Core.DISET.RPCClient                      import RPCClient
taskClient = RPCClient('WorkloadManagement/TaskManager')

def showJob(jobID):
  result = taskClient.getTaskIDFromJob(jobID)
  if not result['OK']:
    print 'Get task ID error: %s' % result['Message']
    return

  if not result['Value']:
    print 'No task ID found for job: %s' % jobID
    return
  taskID = result['Value'][0]

  result = taskClient.getJobInfo(jobID)
  if not result['OK']:
    print 'Get job info error: %s' % result['Message']
    return

  jobInfo = result['Value']

  print 'JobID : %s' % jobID

  print '- TaskID : %s' % taskID

  for k,v in jobInfo.items():
    if type(v) == type([]):
      v = ', '.join(v)
    print '- %s : %s' % (k, v)

def main():
  for jobID in options:
    jobID = int(jobID)
    showJob(jobID)
    print '\n'

  print 'Totally %s job(s) displayed' % len(options)

if __name__ == '__main__':
  main()
