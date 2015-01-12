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
  result = taskClient.getJobInfo(jobID)
  if not result['OK']:
    print 'Get job info error: %s' % result['Message']
    return

  print 'JobID : %s' % jobID

  for k,v in result['Value'].items():
    print '%s : %s' % (k, v)

  print '\n'

def main():
  for jobID in options:
    jobID = int(jobID)
    showJob(jobID)

if __name__ == '__main__':
  main()
