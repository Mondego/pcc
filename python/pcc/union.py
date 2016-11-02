'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import spacetime_property
from _utils import build_required_attrs

class union(object):
    def __init__(self, *types):
        # Classes that it is going to be a union of.
        self.dimensions = set()
        self.dimension_names = set()
        for tp in types:
            if not hasattr(tp, "__dimensions__"):
                tp.__dimensions__ = set()
                tp.__dimensions_name__ = set()
                for attr in dir(tp):
                    if type(getattr(tp, attr)) == spacetime_property:
                        tp.__dimensions__.add(getattr(tp, attr))
                        tp.__dimensions_name__.add(attr)
        self.dimension_names = types[0].__dimensions_name__
        for tp in types[1:]:
            self.dimension_names.intersection_update(tp.__dimensions_name__)
        for dimension in types[0].__dimensions__:
            if dimension._name in self.dimension_names:
                self.dimensions.add(dimension)
            
        self.types = types
        self.real_types = [tp.__realname__ for tp in types]

    def __call__(self, actual_class):
        build_required_attrs(actual_class)
        # actual_class the class that is being passed from application.
        actual_class.__dimensions__ = self.dimensions
        actual_class.__dimensions_name__ = self.dimension_names
        # The pcc union class being cooked right here.
        
        actual_class.__dependent_type__ = True
        actual_class.__ENTANGLED_TYPES__ = self.types
        actual_class.__PCC_BASE_TYPE__ = False
        actual_class.__pcc_bases__ = set(self.types).union(actual_class.__pcc_bases__
                                        if hasattr(actual_class, "__pcc_bases__") else
                                        set())
        actual_class.__start_tracking__ = False
        actual_class.__dimensions__ = actual_class.__dimensions__ if hasattr(actual_class, "__dimensions__") else set()
        actual_class.__dimensions_name__ = actual_class.__dimensions_name__ if hasattr(actual_class, "__dimensions_name__") else set()
        actual_class.__pcc_union__ = True
        actual_class.__pcc_type__ = "union"
        actual_class.__real_types__ = self.real_types
        return actual_class