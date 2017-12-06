'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.utils._utils import build_required_attrs
from rtypes.pcc.utils.enums import PCCCategories


def pcc_set(actual_class):
    build_required_attrs(actual_class, PCCCategories.pcc_set, list())
    return actual_class
