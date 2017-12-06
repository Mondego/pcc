'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.utils._utils import build_required_attrs
from rtypes.pcc.utils.enums import PCCCategories


class subset(object):
    def __init__(self, of_class):
        self.type = of_class

    def __call__(self, actual_class):
        build_required_attrs(
            actual_class, PCCCategories.subset, [self.type])
        return actual_class
