'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from set import pcc_set, PCCMeta

class projection(object):
    def __init__(self, of_class, *dimensions):
        # Class that it is going to be a projection of.
        self.type = of_class
        self.dimensions = dimensions

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        for dimension in self.dimensions:
            setattr(actual_class, dimension._name, dimension)

        # The pcc projection class being cooked right here. 
        class _Projection(actual_class):
            __realname__ = actual_class.__name__
            actual_class.__realname__ = __realname__
        
            __metaclass__ = PCCMeta(actual_class)
            __dependent_type__ = True
            __ENTANGLED_TYPES__ = [self.type]
            __PCC_BASE_TYPE__ = False
            __pcc_bases__ = set([self.type]).union(
                    actual_class.__pcc_bases__ 
                    if hasattr(actual_class, "__pcc_bases__") else 
                    set())
            __start_tracking__ = False
            __pcc_projection__ = True
            
            __dimensions__ = actual_class.__dimensions__ if hasattr(actual_class, "__dimensions__") else set()
            
            @staticmethod
            def Class():
                # Not sure if this should be exposed, 
                # as then people can create objects fromt this
                # useful for inheriting from class directly though.
                return actual_class

            @staticmethod
            def __create_pcc__(change_type_fn, collection, param = tuple()):
                return [change_type_fn(item, actual_class) for item in collection]
             
        return _Projection