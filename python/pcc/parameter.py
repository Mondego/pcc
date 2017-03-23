'''
Create on Feb 27, 2016

@author: Rohan Achar
'''

class ParameterMode(object):
    # default is collection
    Singleton = "singleton"
    Collection = "collection"

class parameter(object):
    def __init__(self, *types, **kwargs):
        self._types = types
        self._mode = kwargs["mode"] if "mode" in kwargs else ParameterMode.Collection
         

    def __call__(self, pcc_class):
        # parameter should be on pcc classes only
        if len(pcc_class.mro()) < 2:
            raise TypeError("Parameter type must derive from some type")
        if not hasattr(pcc_class, "__parameter_types__"):
            pcc_class.__parameter_types__ = dict()
        pcc_class.__parameter_types__.setdefault(self._mode, list()).extend(self._types)
        pcc_class.__pcc_param__ = True
        return pcc_class