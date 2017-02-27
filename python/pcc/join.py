'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import spacetime_property
from _utils import build_required_attrs

class join(object):
    def __init__(self, *classes):
        # List of classes that are part of join
        # should create a class when it gets called
        self.types = classes

    def __call__(self, actual_class):
        build_required_attrs(actual_class)
        # actual_class the class that is being passed from application.
        actual_class.__dependent_type__ = True
        actual_class.__ENTANGLED_TYPES__ = self.types
        actual_class.__PCC_BASE_TYPE__ = False
        actual_class.__pcc_bases__ = set(self.types)
        actual_class.__pcc_join__ = True
        for tp in self.types:
                if hasattr(tp, "__pcc_bases__"):
                    actual_class.__pcc_bases__.update(tp.__pcc_bases__)

        actual_class.__start_tracking__ = False
        actual_class.__pcc_type__ = "join"
        return actual_class
