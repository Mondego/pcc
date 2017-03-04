from copy import deepcopy
from itertools import product
class container(object):
    def __init__(self):
        pass

    
def __subset_generic_query(actual_class, collection, param):
    if len(collection) > 1:
        raise TypeError("Cannot create subset %s from more that one collection. Use params = (<params>,...) to use parameters" % actual_class.__realname__)
    if hasattr(actual_class, "__grouped_pcc__") and actual_class.__grouped_pcc__:
        return __convert_to_grp(actual_class, [item for item in collection[0] if actual_class.__predicate__(*([item] if param == tuple() else [item] + list(param)))])
    return [change_type(item, actual_class) for item in collection[0] if actual_class.__predicate__(*([item] if param == tuple() else [item] + list(param)))]

def __join_generic_query(actual_class, collections, param):
    return [actual_class(*(list(one_cross) + list(param))) for one_cross in product(*collections) if actual_class.__predicate__(*(list(one_cross) + list(param)))]

def __union_generic_query(actual_class, collections, *param):
    if hasattr(actual_class, "__grouped_pcc__") and actual_class.__grouped_pcc__:
        return __convert_to_grp(actual_class, [item 
                for collection in collections 
                    for item in collection 
                        if type(item).__realname__ in actual_class.real_types])
    
    return [change_type(item, actual_class) 
            for collection in collections 
                for item in collection 
                    if type(item).__realname__ in actual_class.real_types]

def __generic_conversion(actual_class, collection, *param):
    if hasattr(actual_class, "__grouped_pcc__") and actual_class.__grouped_pcc__:
        return __convert_to_grp(actual_class, collection[0])
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
    items = []
    if hasattr(actual_class, "__query__"):
        items = [change_type(item, actual_class) for item in actual_class.__query__(*(list(collections) + list(params)))]
    else:
        items = __generic_query[actual_class.__pcc_type__](actual_class, collections, params)
    if hasattr(actual_class, "__orderby__"):
        items = sorted(items, key = actual_class.__orderby__)
    if hasattr(actual_class, "__limit__"):
        items = items[:actual_class.__limit__]
    if hasattr(actual_class, "__distinct__"):
        dict_by_prop = {}
        for item in items:
            if item.__distinct__ not in dict_by_prop:
                dict_by_prop[item.__distinct__] = item
        items = dict_by_prop.values()

    if hasattr(actual_class, "__post_process__"):
        items = [actual_class.__post_process__(item) for item in items]
    return items

def change_type(obj, totype):
    new_obj = container()
    new_obj.__dict__ = obj.__dict__
    new_obj.__class__ = totype
    return new_obj

def __convert_to_grp(actual_class, list_of_objs):
    agg_dict = dict()
    final_result = list()
    for obj in list_of_objs:
        agg_dict.setdefault(getattr(obj, actual_class.__primarykey__._name), list()).append(obj)
    class container(object):
        pass
    for groupkey, objs_for_grp in agg_dict.items():
        obj = change_type(container(), actual_class)
        obj.__primarykey__ = groupkey
        for dim in actual_class.__dimensions__:
            setattr(obj, dim._name, dim.on_call_func([getattr(gobj, dim._target_prop._name) for gobj in objs_for_grp]))
        final_result.append(obj)
    return final_result
        