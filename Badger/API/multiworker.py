#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang

import threading
import time
import random

class IWorker(object):

  def get_file_list(self):
    raise NotImplementedError

  def Do(self, item):
    raise NotImplementedError

class UploadWorker(IWorker):

  def get_file_list(self):
    return map(str,range(10))

  def Do(self, item):
    time.sleep(random.randint(0,9))
    print item

class MultiWorker(object):

  def __init__(self, worker, pool_size=5):
    self.max_pool_size = pool_size
    self.pool = []
    self.worker = worker
    self.it = iter(worker.get_file_list())

  def check_pool(self):
    return self.max_pool_size - len(self.pool)

  def clear_pool(self):
    for t in self.pool[:]:
      if not t.isAlive():
        self.pool.remove(t)

  def get_file(self):
    try:
      return self.it.next()
    except StopIteration:
      return None
    pass

  def main(self):
    while True:
      self.clear_pool()
      residual = self.check_pool()
      if not residual:
        continue
      for i in range(residual):
        f = self.get_file()
        if f is None:
          return


        t = threading.Thread( target=worker.Do, args=(f,) )
        self.pool.append(t)
        t.start()

      #break



if __name__ == "__main__":

  worker = UploadWorker()
  #print worker.get_file_list()
  #print worker.Do("1")

  mw = MultiWorker(worker)
  #for i in range(11):
  #  print mw.check_pool()
  #  print mw.get_file()

  mw.main()
