'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
import uuid
from threading import currentThread
from recursive_dictionary import RecursiveDictionary
from abc import ABCMeta, abstractmethod

def get_type(obj):
    # both iteratable/dictionary + object type is messed up. Won't work.
    try:
        if hasattr(obj, "__dependent_type__"):
            return "dependent"
        if dict in type(obj).mro():
            return "dictionary"
        if hasattr(obj, "__iter__"):
            print obj
            return "collection"
        if len(set([float, int, str, unicode, type(None)]).intersection(set(type(obj).mro()))) > 0:
            return "primitive"
        if hasattr(obj, "__dict__"):
            return "object"
    except TypeError, e:
        return "unknown"
    return "unknown"

class spacetime_property(property):
    change_tracker = RecursiveDictionary()
    GLOBAL_TRACKER = False
    def __init__(self, tp, fget, fset = None, fdel = None, doc = None):
        setattr(self, "_type", tp)
        setattr(self, "_dimension", True)
        setattr(self, "_name", fget.func_name)
        setattr(self, "change", dict())
        setattr(self, "_primarykey", None)

        # the next 2 is only for dataframe use
        setattr(self, "_dataframe_data", set())
        property.__init__(self, fget, fset, fdel, doc)

    def setter(self, fset):
        prop = spacetime_property(self._type, self.fget, fset)
        for a in self.__dict__:
            setattr(prop, a, self.__dict__[a])
        return prop

    def __copy__(self):
        prop = Property(self.fget, self.fset)
        prop.__dict__.update(self.__dict__)
        return prop

    def __set__(self, obj, value, bypass = False):
        property.__set__(self, obj, value)
        if not hasattr(obj, "__start_tracking__"):
            return
        if hasattr(obj, "_dataframe_data") and obj._dataframe_data and hasattr(obj, "__primarykey__") and obj.__primarykey__:
            if obj.__start_tracking__:
                tp_getter, tr, gr, rr = obj._dataframe_data
                tp_obj = tp_getter(obj.__class__)
                #rc(obj.__primarykey__, self, value, tp_obj)
                records = (gr(tp_obj, obj.__primarykey__, {self: value}))
                records.extend(tr(tp_obj, {obj.__primarykey__: (obj, {self: value})}, to_be_converted = True))
                rr(records)
                    
        if not obj.__start_tracking__ or bypass:
            if self._primarykey and value == None:
                value = str(uuid.uuid4())
                obj._primarykey = self
            property.__set__(self, obj, value)
            return
        if not self._primarykey and "_primarykey" != self._name and spacetime_property.GLOBAL_TRACKER:
            type_name = get_type(value)
            store_value = value
            if type_name == "dependent":
                return
            elif type_name == "object":
                store_value = dict(value.__dict__)

            spacetime_property.change_tracker.setdefault(
                currentThread().getName(), RecursiveDictionary()).setdefault(
                    obj.__class__, RecursiveDictionary()).setdefault(
                        obj.__primarykey__, dict())[self._name] = store_value

class primarykey(object):
    def __init__(self, tp = None, default = True):
        self.type = tp if tp else "primitive"
        self.default = default

    def __call__(self, func):
        x = spacetime_property(self.type, func)
        
        x._primarykey = True
        return x

class dimension(object):
    def __init__(self, tp = None):
        self.type = tp if tp else "primitive"

    def __call__(self, func):
        x = spacetime_property(self.type, func)
        return x
    
class aggregate_property(property):
    def __init__(self, prop, on_call_func, fget = None, fset = None, fdel = None, doc = None):
        setattr(self, "_name", fget.func_name)
        setattr(self, "_target_prop", prop)
        self.on_call_func = on_call_func
        return property.__init__(self, fget, fset, fdel, doc)

    def setter(self, fset):
        prop = aggregate_property(self._target_prop, self.on_call_func, self.fget, fset)
        for a in self.__dict__:
            setattr(prop, a, self.__dict__[a])
        return prop

    

class aggregate(object):
    __metaclass__ = ABCMeta
    def __init__(self, prop):
        setattr(self, "prop", prop)
        if not isinstance(prop, spacetime_property):
            raise TypeError("Cannot create aggregate type with given property")

    def __call__(self, func):
        return aggregate_property(self.prop, self.on_call, func)

    @abstractmethod
    def on_call(self, list_of_target_prop):
        raise NotImplementedError("Abstract class implementation. Not to be called.")

class summation(aggregate):
    def on_call(self, list_of_target_prop):
        return sum(list_of_target_prop)

class count(aggregate):
    def on_call(self, list_of_target_prop):
        return len(list_of_target_prop)

class average(aggregate):
    def on_call(self, list_of_target_prop):
        return float(sum(list_of_target_prop)) / float(len(list_of_target_prop))

class maximum(aggregate):
    def on_call(self, list_of_target_prop):
        return max(list_of_target_prop)

class minimum(aggregate):
    def on_call(self, list_of_target_prop):
        return min(list_of_target_prop)
