
class parameter(object):
    def __init__(self, *types):
        self._types = types

    def __call__(self, pcc_class):
        # parameter should be on pcc classes only
        if len(pcc_class.mro()) < 2:
            raise TypeError("Parameter type must derive from some type")
        pcc_class.__parameter_types__ = self._types
        return pcc_class