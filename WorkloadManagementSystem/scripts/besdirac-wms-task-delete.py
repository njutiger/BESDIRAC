#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Delete task and all jobs in the task

Usage:
   %s [option] ... [TaskID] ...
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getUnprocessedSwitches()
options = Script.getPositionalArgs()

from BESDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

def deleteTask(taskID):
  result = taskClient.deleteTask(taskID)
  if not result['OK']:
    print 'Delete task error: %s' % result['Message']
    return
  print 'Task %s deleted' % taskID

def main():
  if len(options) < 1:
    Script.showHelp()
    return

  for taskID in options:
    taskID = int(taskID)
    deleteTask(taskID)
    print ''

if __name__ == '__main__':
  main()
