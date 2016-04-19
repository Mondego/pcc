from abc import ABCMeta, abstractmethod

class IApplication(object):
  __metaclass__ = ABCMeta 
  
  def is_done(self):
    return False

  @abstractmethod
  def initialize(self):
    pass

  @abstractmethod
  def update(self):
    pass

  @abstractmethod
  def shutdown(self):
    pass
