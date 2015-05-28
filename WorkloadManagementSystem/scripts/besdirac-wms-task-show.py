#!/usr/bin/env python

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

from BESDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

from DIRAC.Core.DISET.RPCClient                      import RPCClient
jobmonClient = RPCClient('WorkloadManagement/JobMonitoring')

def showTask(taskID):
  outFields = ['TaskID','TaskName','Status','Owner','OwnerDN','OwnerGroup','CreationTime','UpdateTime','JobGroup','Site','Progress','Info']
  result = taskClient.getTask(taskID)
  if not result['OK']:
    print 'Get task error: %s' % result['Message']
    return
  task = result['Value']

  for k in outFields:
    if k in ['Progress', 'Info']:
      print '\n== %s ==' % k
      for kp,vp in sorted(task[k].iteritems(), key=lambda d:d[0]):
        if type(vp) == type([]):
          vp = ', '.join(vp)
        print '%s : %s' % (kp, vp)
    else:
      print '%s : %s' % (k, task[k])

def showTaskJobs(taskID):
  result = taskClient.getTaskJobs(taskID)
  if not result['OK']:
    print 'Get task jobs error: %s' % result['Message']
    return
  jobIDs = result['Value']
  print '== Jobs =='
  if not jobIDs:
    print 'No jobs found'
    return

  for jobID in jobIDs:
    print jobID,
  print ''

  result = jobmonClient.getJobs( { 'JobID': jobIDs, 'Status': ['Failed'] } )
  if not result['OK']:
    print 'Get task failed jobs error: %s' % result['Message']
    return
  print '\n== Jobs (Failed) =='
  for jobID in result['Value']:
    print jobID,

def showTaskHistories(taskID):
  result = taskClient.getTaskHistories(taskID)
  if not result['OK']:
    print 'Get task histories error: %s' % result['Message']
    return

  print '\n== Job Histories =='
  for status, statusTime, description in result['Value']:
    print '%-16s %-24s %s' % (status, statusTime, description)

def main():
  for taskID in options:
    print '='*80
    taskID = int(taskID)
    showTask(taskID)
    print ''
    showTaskJobs(taskID)
    print ''
    showTaskHistories(taskID)
    print ''

if __name__ == '__main__':
  main()
