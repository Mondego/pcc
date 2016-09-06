'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import spacetime_property
from set import PCCMeta

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
        # actual_class the class that is being passed from application.
        actual_class.__dimensions__ = self.type.__dimensions__
        actual_class.__dimensions_name__ = self.type.__dimensions_name__
        # The pcc subset class being cooked right here.
        class _Subset(object):
            __realname__ = actual_class.__name__
            __metaclass__ = PCCMeta(actual_class)
            __dependent_type__ = True
            __ENTANGLED_TYPES__ = [self.type]
            __PCC_BASE_TYPE__ = False
            __pcc_bases__ = set([self.type]).union(actual_class.__pcc_bases__
                    if hasattr(actual_class, "__pcc_bases__") else
                    set())
            __start_tracking__ = False
            __dimensions__ = actual_class.__dimensions__ if hasattr(actual_class, "__dimensions__") else set()
            __dimensions_name__ = actual_class.__dimensions_name__ if hasattr(actual_class, "__dimensions_name__") else set()
            __pcc_subset__ = True

            @staticmethod
            def Class():
                # Not sure if this should be exposed,
                # as then people can create objects fromt this
                # useful for inheriting from class directly though.
                return actual_class

            @staticmethod
            def __query__(collection, *param):
                if hasattr(actual_class, "__query__"):
                    return actual_class.__query__(collection, *param)
                return [item for item in collection if actual_class.__predicate__(item, *param)]

            @staticmethod
            def __predicate__(*args, **kwargs):
                return actual_class.__predicate__(*args, **kwargs)

            @staticmethod
            def __create_pcc__(change_type_fn, collection, param = tuple()):
                return [change_type_fn(item, actual_class) for item in _Subset.__query__(collection, *param)]

        return _Subset