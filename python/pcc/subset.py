'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import spacetime_property
from _utils import build_required_attrs

class subset(object):
    def __init__(self, of_class):
        # Class that it is going to be a subset of.
        if not hasattr(of_class, "__dimensions__"):
            of_class.__dimensions__ = set()
            of_class.__dimensions_name__ = set()
            for attr in dir(of_class):
                if type(getattr(of_class, attr)) == spacetime_property:
                    of_class.__dimensions__.add(getattr(of_class, attr))
                    of_class.__dimensions_name__.add(attr)
            
        self.type = of_class

    def __call__(self, actual_class):
        build_required_attrs(actual_class)
        if not actual_class.__grouped_pcc__:
            for dim in self.type.__dimensions__:
                setattr(actual_class, dim._name, dim)
                actual_class.__dimensions__.add(dim)
                actual_class.__dimensions_name__.add(dim._name)
                if hasattr(dim, "_primarykey") and getattr(dim, "_primarykey") != None:
                    actual_class.__primarykey__ = dim
                    setattr(actual_class, dim._name, dim)
            actual_class.__primarykey__ = self.type.__primarykey__
            actual_class.__dimensions__.update(self.type.__dimensions__)
            actual_class.__dimensions_name__.update(self.type.__dimensions_name__)
        
        actual_class.__PCC_BASE_TYPE__ = False
        actual_class.__dependent_type__ = True
        actual_class.__pcc_bases__ = set([self.type]).union(actual_class.__pcc_bases__
                                        if hasattr(actual_class, "__pcc_bases__") else
                                        set())
        actual_class.__ENTANGLED_TYPES__ = [self.type]
        actual_class.__start_tracking__ = False
        actual_class.__pcc_subset__ = True
        actual_class.__pcc_type__ = "subset"
        return actual_class
