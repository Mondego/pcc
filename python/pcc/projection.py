'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from set import pcc_set
from _utils import build_required_attrs

class projection(object):
    def __init__(self, of_class, *dimensions):
        # Class that it is going to be a projection of.
        self.type = of_class
        self.dimensions = dimensions

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        for dimension in self.dimensions:
            setattr(actual_class, dimension._name, dimension)
        build_required_attrs(actual_class)
        actual_class.__dependent_type__ = True
        actual_class.__ENTANGLED_TYPES__ = [self.type]
        actual_class.__PCC_BASE_TYPE__ = False
        actual_class.__pcc_bases__ = set([self.type]).union(
                actual_class.__pcc_bases__ 
                if hasattr(actual_class, "__pcc_bases__") else 
                set())
        actual_class.__start_tracking__ = False
        actual_class.__pcc_projection__ = True
            
        actual_class.__pcc_type__ = "projection"
            
            #@staticmethod
            #def Class():
            #    # Not sure if this should be exposed, 
            #    # as then people can create objects fromt this
            #    # useful for inheriting from class directly though.
            #    return actual_class

            #@staticmethod
            #def __create_pcc__(change_type_fn, collection, param = tuple()):
            #    return [change_type_fn(item, actual_class) for item in collection]
             
        return actual_class