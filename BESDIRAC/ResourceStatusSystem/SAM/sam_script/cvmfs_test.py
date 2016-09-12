#!/usr/bin/env python

import socket, subprocess, time

hostname = socket.gethostname()

commands = ( 'ls /cvmfs/boss.cern.ch/',  'ls /cvmfs/bes.ihep.ac.cn' )
stdoutList = []
stderrList = []

start = time.time()
for command in commands:
  subp = subprocess.Popen( command, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
  stdout, stderr = subp.communicate()
  stdoutList.append( stdout )
  stderrList.append( stderr )
runningTime = time.time() - start

print 'Host Name :', hostname
print 'Running Time :', runningTime
print '\n'
print '==============================Standard Output==============================\n'
for i in range(len(commands)):
  if stdout[i] != '':
    print '>>> %s\n' % commands[i]
    print stdout[i]
print '\n'
print '==============================Standard Error===============================\n'
for i in range(len(commands)):
  if stderr[i] != '':
    print '>>> %s\n' % commands[i]
    print stderr[i]
