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

from BESDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

def rescheduleTask(taskID, status=[]):
  result = taskClient.rescheduleTask(taskID, status)
  if not result['OK']:
    print 'Reschedule task error: %s' % result['Message']
    return
  print 'Task %s rescheduled' % taskID

def main():
  for taskID in options:
    taskID = int(taskID)
    rescheduleTask(taskID, ['Failed'])
    print ''

if __name__ == '__main__':
  main()
