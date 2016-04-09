import logging
import datetime
import time
from multiprocessing import Process

from simplestore import store
from localstore import localstore

class framework(Process):
  '''
  classdocs
  '''
  def __init__(self, custom_store = None, settings = None, group = None, target = None, name = None, args = (), kwargs = {}):
    self.app = None
    self.appname = None
    self.settings = None
    self.interval = None
    self.typemap = None
    self.store = custom_store if custom_store else store()
    self.localstore = localstore()
    self.__logger = logging.getLogger(__name__)
    super(framework, self).__init__(group, target, name, args, kwargs)
    self.daemon = True
    
  
  def pull(self):
    '''
    Function that pulls the required data from the datasets
    '''
    for bucket in self.typemap:
      for t in self.typemap[bucket]:
        (new, mod, deleted) = self.store.getupdated(
            t, self.appname, tracked_only = (bucket == "tracked"))
        self.localstore.setchanges(new, mod, deleted)

  def push(self):
    self.store.update_all(self.localstore.getchanges(), self.appname)
    self.localstore.clearchangelist()

  def __executeonce():
    # get data from store.
    self.pull()
    # run application tick.
    self.app.update()
    # push the updates back to store.
    self.push()

  def __gettypemap(self, app):
    raise Exception("Implement this now!")
    pass

  def attach(self, app):
    self.app = app
    self.appname = app.__class__.__name__
    self.typemap = self.__gettypemap(self.app)

  def initialize_app(self):
    # Application starts up.
    self.app.initialize()
    # First push
    self.push()
    self.__logger.info("Application %s finished initialization", self.app.__name__)

  def run(self):
    if self.app == None or self.app.framework == None:
      raise RuntimeError("attach app to framework, and framework to app")
    self.initialize_app()
    iteration = 0
    while not self.app.start_shutdown():
      stime = time.time()
      self.__executeonce()
      etime = time.time()
      delta_secs = (etime - stime)

      if delta_secs < self.interval :
        time.sleep(self.interval - delta_secs)
      else:
        self.__Logger.warn("[%s]: Exceeded interval time by %s at iteration %s" , self.app.__module__, delta_secs * 1000, iteration)
      iteration += 1

  def get(self, t_or_id):
    return self.localstore.get(t_or_id)

  def put(self, obj):
    return self.localstore.put(obj)

  def delete(self, obj):
    return self.localstore.delete(obj)