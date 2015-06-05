#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Reschedule jobs in the task. Only reschedule failed jobs by default

Usage:
   %s [option] ... [TaskID] ...
""" % Script.scriptName )

Script.registerSwitch( "a",  "all",        "Reschdule all jobs in the task" )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()
options = Script.getPositionalArgs()

from BESDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

def rescheduleTask(taskID, status=[]):
  result = taskClient.rescheduleTask(taskID, status)
  if not result['OK']:
    print 'Reschedule task error: %s' % result['Message']
    return
  print 'Task %s rescheduled' % taskID

def main():
  if len(options) < 1:
    Script.showHelp()
    return

  status = ['Failed']
  for arg in args:
    (switch, val) = arg
    if switch == 'a' or switch == 'all':
      status = []

  for taskID in options:
    taskID = int(taskID)
    rescheduleTask(taskID, status)
    print ''

if __name__ == '__main__':
  main()
