'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.utils._utils import build_required_attrs
from rtypes.pcc.utils.enums import PCCCategories


class join(object):
    def __init__(self, *classes):
        # List of classes that are part of join
        # should create a class when it gets called
        self.types = classes

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        build_required_attrs(actual_class, PCCCategories.join, self.types)
        return actual_class
