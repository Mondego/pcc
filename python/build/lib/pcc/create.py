from copy import deepcopy
from set import PCCMeta

def create(tp, *args, **kwargs):
    tocopy = kwargs["copy"] if "copy" in kwargs else False
    def change_type(obj, totype):
        class container(totype):
            __metaclass__ = PCCMeta(totype)
            __original_class__ = totype
            def __init__(self):
                pass

        new_obj = container()
        if tocopy:
            for dimension in container.__dimensions_name__.intersection(obj.__class__.__dimensions_name__):
                setattr(new_obj, dimension, getattr(obj, dimension))
        else:
            new_obj.__dict__ = obj.__dict__
        return new_obj
    params = tuple()
    if hasattr(tp, "__parameter_types__"):
        try:
            params = kwargs["params"]
        except KeyError:
            raise TypeError("Cannot create %s without params. Pass params using params keyword argument" % tp.__realname__)
    copyargs = deepcopy(args) if tocopy else args
    if not isinstance(tp, type):
            raise SyntaxError("%s is not a type" % tp)
    if len(args) < 1:
        raise SyntaxError("No objects of type %s" % tp.__name__)
    return tp.__create_pcc__(change_type, *copyargs, param = params) if hasattr(tp, "__create_pcc__") else (copyargs[0] if len(copyargs) == 1 else copyargs)