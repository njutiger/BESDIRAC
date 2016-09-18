#!/usr/bin/env python
  2 
  3 import socket, subprocess, time
  4 
  5 hostname = socket.gethostname()
  6 
  7 commands = """
  8 ls /cvmfs/boss.cern.ch/
  9 """
 10 
 11 start = time.time()
 12 subp = subprocess.Popen( commands, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
 13 stdout, stderr = subp.communicate()
 14 runningTime = time.time() - start
 15 
 16 print 'Host Name :', hostname
 17 print 'Running Time :', runningTime
 18 print '\n'
 19 if stdout:
 20     print '==============================Standard Output==============================\n'
 21     print stdout
 22     print '\n'
 23 if stderr:
 24     print '==============================Standard Error===============================\n'
 25     print stderr
