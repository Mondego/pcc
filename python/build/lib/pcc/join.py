from set import PCCMeta
from attributes import spacetime_property

class join(object):
    def __init__(self, *classes):
        # List of classes that are part of join
        # should create a class when it gets called
        self.types = classes

    def __call__(self, actual_class):
        # actual_class the class that is being passed from application.
        class _Join(object):
            __realname__ = actual_class.__name__
            __metaclass__ = PCCMeta(actual_class)
            __dependent_type__ = True
            __ENTANGLED_TYPES__ = self.types
            __PCC_BASE_TYPE__ = False
            __pcc_bases__ = set([self.types])
            for tp in self.types:
                 if hasattr(tp, "__pcc_bases__"):
                        __pcc_bases__.update(tp.__pcc_bases__)

            __start_tracking__ = False
            
            @staticmethod
            def __create_permutation(args):
                if len(args) == 1:
                    return [(item,) for item in args[0]]
                return [tuple((item,) + p) for item in args[0] for p in _Join.__create_permutation(args[1:])]
            
            @staticmethod
            def Class():
                # Not sure if this should be exposed, 
                # as then people can create objects fromt this
                # useful for inheriting from class directly though.
                return actual_class

            @staticmethod
            def __pcc_query__(*args, **kwargs):
                params = kwargs["param"] if "param" in kwargs else tuple()
                if hasattr(actual_class, "__query__"):
                    return actual_class.__query__(*tuple(args + params))
                
                collections = _Join.__create_permutation(args)
                return [item for item in collections if actual_class.__predicate__(*tuple(item + params))]
            
            @staticmethod
            def __predicate__(*args, **kwargs):
                return actual_class.__predicate__(*args, **kwargs)

            @staticmethod
            def __create_pcc__(change_type_fn, *args, **kwargs):
                try:
                    return [actual_class(*item) for item in _Join.__pcc_query__(*args, **kwargs)]
                except TypeError:
                    return [actual_class(item) for item in _Join.__pcc_query__(*args, **kwargs)]
        
        return _Join