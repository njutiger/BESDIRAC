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

from BESDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

def showJobs(jobIDs):
  outFields = ['TaskID', 'JobID', 'Info']
  result = taskClient.showJobs(jobIDs, outFields)
  if not result['OK']:
    print 'Show jobs error: %s' % result['Message']
    return

  if not result['Value']:
    print 'No task found for jobs: %s' % jobIDs
    return

  for v in result['Value']:
    taskID = v[0]
    jobID = v[1]
    jobInfo = json.loads(v[2])

    print 'JobID : %s' % jobID

    print '- TaskID : %s' % taskID

    for k,v in sorted(jobInfo.iteritems(), key=lambda d:d[0]):
      if type(v) == type([]):
        v = ', '.join(v)
      print '- %s : %s' % (k, v)

    print ''

def main():
  jobIDs = []
  for jobID in options:
    jobIDs.append(int(jobID))

  showJobs(jobIDs)

  print 'Totally %s job(s) displayed' % len(jobIDs)

if __name__ == '__main__':
  main()
