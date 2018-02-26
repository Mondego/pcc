from copy import deepcopy
from itertools import product
from rtypes.pcc.utils.enums import PCCCategories
from rtypes.pcc.attributes import staticmethod_predicate

class container(object):
    def __init__(self):
        pass

def __run_predicate(actual_class, item, param):
    metadata = actual_class.__rtypes_metadata__
    if not metadata.is_new_type_predicate:
        return actual_class.__predicate__(
            *([item] if param == tuple() else [item] + list(param)))
    dimvalues = [
        getattr(item, dim._name)
        for dim in actual_class.__predicate__.dimensions]
    return actual_class.__predicate__(
        *(dimvalues if param == tuple() else dimvalues + list(param)))

def __subset_generic_query(actual_class, collection, param):
    if len(collection) > 1:
        raise TypeError(
            "Cannot create subset %s from more that one collection. "
            "Use params = (<params>,...) "
            "to use parameters" % actual_class.__realname__)
    if actual_class.__rtypes_metadata__.group_dimensions:
        return __convert_to_grp(
            actual_class,
            [item for item in collection[0]
             if __run_predicate(actual_class, item, param)])
    return [change_type(item, actual_class)
            for item in collection[0]
            if __run_predicate(actual_class, item, param)]

def setup_join_obj(cls, objs):
    final_obj = change_type(container(), cls)
    used_namspaces = set()
    for obj in objs:
        found = False
        for ns_obj in cls.__rtypes_metadata__.namespace_dimensions:
            if isinstance(obj, ns_obj._type) and ns_obj._name not in used_namspaces:
                used_namspaces.add(ns_obj._name)
                setattr(final_obj, ns_obj._name, obj)
                found = True
                break
        if not found:
            TypeError(
                "Could not find the namespace with the type {0} "
                "to put obj of {1}".format(
                    type(obj), obj))
    return final_obj


def __join_generic_query(actual_class, collections, param):
    return [setup_join_obj(actual_class, one_cross)
            for one_cross in product(*collections)
            if actual_class.__predicate__(*(list(one_cross) + list(param)))]

def __union_generic_query(actual_class, collections, *param):
    parents = actual_class.__rtypes_metadata__.parents
    if actual_class.__rtypes_metadata__.group_dimensions:
        return __convert_to_grp(
            actual_class, [
                item
                for collection in collections
                for item in collection 
                if type(item).__rtypes_metadata__ in parents])
    
    return [change_type(item, actual_class) 
            for collection in collections
            for item in collection
            if type(item).__rtypes_metadata__ in parents]

def __generic_conversion(actual_class, collection, *param):
    if actual_class.__rtypes_metadata__.group_dimensions:
        return __convert_to_grp(actual_class, collection[0])
    return [change_type(item, actual_class) for item in collection[0]]

def __pcc_set_generic_query(actual_class, collection, *param):
    return collection[0]

__generic_query = {
    PCCCategories.pcc_set: __pcc_set_generic_query,
    PCCCategories.subset: __subset_generic_query,
    PCCCategories.join: __join_generic_query,
    PCCCategories.union: __union_generic_query,
    PCCCategories.projection: __generic_conversion,
}

def create(tp, *args, **kwargs):
    params = tuple()
    if not isinstance(tp, type):
        raise SyntaxError("%s is not a type" % tp)
    if len(args) < 1:
        raise SyntaxError("No objects of type %s" % tp.__name__)
    if not hasattr(tp, "__rtypes_metadata__"):
        raise TypeError(
            "Cannot create non PCC collections ({0})".format(repr(tp)))
    metadata = tp.__rtypes_metadata__
    if metadata.parameter_types:
        try:
            params = kwargs["params"]
        except KeyError:
            raise TypeError(
                "Cannot create %s without params. Pass params using params "
                "keyword argument" % tp.__realname__)
    return __create_pcc(tp, params, args)

def __create_pcc(actual_class, params, collections):
    items = []
    metadata = actual_class.__rtypes_metadata__
    if hasattr(actual_class, "__query__"):
        items = [change_type(item, actual_class)
                 for item in actual_class.__query__(
                     *(list(collections) + list(params)))]
    else:
        citems = collections
        for category in metadata.category_execution_order:
            citems = (__generic_query[category](
                actual_class, citems, params),)
        items = citems[0]
    if hasattr(actual_class, "__order_by__"):
        items = sorted(items, key = actual_class.__order_by__)
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
    metadata = actual_class.__rtypes_metadata__
    agg_dict = dict()
    final_result = list()
    for obj in list_of_objs:
        agg_dict.setdefault(
            getattr(obj, metadata.group_by._name), list()).append(obj)
    for groupkey, objs_for_grp in agg_dict.items():
        obj = change_type(container(), actual_class)
        obj.__primarykey__ = groupkey
        obj.__group_by__ = groupkey
        for dim in metadata.group_dimensions:
            setattr(
                obj, dim._name, dim.on_call_func(
                    [getattr(gobj, dim._target_prop._name)
                     for gobj in objs_for_grp]))
        final_result.append(obj)
    return final_result
