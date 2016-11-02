'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import primarykey, dimension, spacetime_property
from types import FunctionType
from copy import deepcopy
from _utils import build_required_attrs

def pcc_set(actual_class):
    build_required_attrs(actual_class)
    actual_class.__PCC_BASE_TYPE__ = True
    actual_class.__dependent_type__ = True
    actual_class.__pcc_bases__ = set()
    actual_class.__ENTANGLED_TYPES__ = list()
    actual_class.__start_tracking__ = False

    return actual_class
