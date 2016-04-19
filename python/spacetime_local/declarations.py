class dummy(object):
  pass

class Producer(object):
  def __init__(self, *types):
    self.types = set(types)

  def __call__(self, actual_class):
    actual_class.__pcc_producing__ = self.types
    d = dummy()
    d.__class__ = actual_class

    return actual_class

class Tracker(object):
  def __init__(self, *types):
    self.types = set(types)

  def __call__(self, actual_class):
    actual_class.__pcc_tracking__ = self.types
    return actual_class

class Getter(object):
  def __init__(self, *types):
    self.types = set(types)

  def __call__(self, actual_class):
    actual_class.__pcc_getting__ = self.types
    return actual_class

class GetterSetter(object):
  def __init__(self, *types):
    self.types = set(types)

  def __call__(self, actual_class):
    actual_class.__pcc_gettingsetting__ = self.types
    return actual_class

class Deleter(object):
  def __init__(self, *types):
    self.types = set(types)

  def __call__(self, actual_class):
    actual_class.__pcc_deleting__ = self.types
    return actual_class
