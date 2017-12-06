'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from rtypes.pcc.utils._utils import build_required_attrs
from rtypes.pcc.utils.enums import PCCCategories


class projection(object):
    def __init__(self, of_class, *dimensions):
        # Class that it is going to be a projection of.
        self.type = of_class
        self.dimensions = dimensions

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        build_required_attrs(
            actual_class, PCCCategories.projection,
            [self.type], self.dimensions)
        return actual_class