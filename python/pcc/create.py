from copy import deepcopy
from itertools import product

def __subset_generic_query(actual_class, collection, param):
    if len(collection) > 1:
        raise TypeError("Cannot create subset %s from more that one collection. Use params = (<params>,...) to use parameters" % actual_class.__realname__)
    return [change_type(item, actual_class) for item in collection[0] if actual_class.__predicate__(*([item] if param == tuple() else [item] + list(param)))]

def __join_generic_query(actual_class, collections, param):
    return [actual_class(*(list(one_cross) + list(param))) for one_cross in product(*collections) if actual_class.__predicate__(*(list(one_cross) + list(param)))]

def __union_generic_query(actual_class, collections, *param):
    return [change_type(item, actual_class) 
            for collection in collections 
                for item in collection 
                    if type(item).__realname__ in actual_class.real_types]

def __generic_conversion(actual_class, collection, *param):
    return [change_type(item, actual_class) for item in collection[0]]

__generic_query = {
    "subset": __subset_generic_query,
    "join": __join_generic_query,
    "union": __union_generic_query,
    "projection": __generic_conversion,
}

def create(tp, *args, **kwargs):
    params = tuple()
    if hasattr(tp, "__parameter_types__"):
        try:
            params = kwargs["params"]
        except KeyError:
            raise TypeError("Cannot create %s without params. Pass params using params keyword argument" % tp.__realname__)
    if not isinstance(tp, type):
        raise SyntaxError("%s is not a type" % tp)
    if len(args) < 1:
        raise SyntaxError("No objects of type %s" % tp.__name__)
    return __create_pcc(tp, params, args)

def __create_pcc(actual_class, params, collections):
    if hasattr(actual_class, "__query__"):
        return [change_type(item, actual_class) for item in actual_class.__query__(*(list(collections) + list(params)))]
    else:
        return __generic_query[actual_class.__pcc_type__](actual_class, collections, params)

def change_type(obj, totype):
    class container(object):
        def __init__(self):
            pass

    new_obj = container()
    new_obj.__dict__ = obj.__dict__
    new_obj.__class__ = totype
    return new_obj