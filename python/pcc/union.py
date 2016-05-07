from attributes import spacetime_property
from set import PCCMeta

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
        self.real_types = [tp.Class() if hasattr(tp, "Class") else tp for tp in types]

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        actual_class.__dimensions__ = self.dimensions
        actual_class.__dimensions_name__ = self.dimension_names
        # The pcc union class being cooked right here.
        class _Union(object):
            __realname__ = actual_class.__name__
            __metaclass__ = PCCMeta(actual_class)
            __dependent_type__ = True
            __ENTANGLED_TYPES__ = self.types
            __PCC_BASE_TYPE__ = False
            __pcc_bases__ = set(self.types).union(actual_class.__pcc_bases__
                    if hasattr(actual_class, "__pcc_bases__") else
                    set())
            __start_tracking__ = False
            __dimensions__ = actual_class.__dimensions__ if hasattr(actual_class, "__dimensions__") else set()
            __dimensions_name__ = actual_class.__dimensions_name__ if hasattr(actual_class, "__dimensions_name__") else set()
            
            @staticmethod
            def Class():
                # Not sure if this should be exposed,
                # as then people can create objects fromt this
                # useful for inheriting from class directly though.
                return actual_class

            @staticmethod
            def __create_pcc__(change_type_fn, *collections, **kwargs):
                return [change_type_fn(item, actual_class) 
                                for collection in collections 
                                    for item in collection 
                                        if type(item).__original_class__ in self.real_types]

        return _Union