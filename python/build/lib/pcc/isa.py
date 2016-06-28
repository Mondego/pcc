from set import pcc_set

class isa(object):
  def __init__(of_cls, *dimensions):
    self.of_cls = of_cls
    self.dimensions = dimensions

  def __call__(actual_class):
    set_cls = pcc_set(actual_class)
    if hasattr(actual_class, "__isa__"):
      actual_class.__isa__.add(of_cls)
      actual_class.__dimensions__.update(set(dimensions))
    return actual_class
