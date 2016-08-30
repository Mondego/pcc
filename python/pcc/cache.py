import logging

from recursive_dictionary import RecursiveDictionary

class Cache(object):
    def __init__(self):
        self.__app_data = RecursiveDictionary()
        self.__app_allowed_types = {}
        self.logger = logging.getLogger(__name__)

    def __type_check(self, tpname):
        if tpname in self.__app_data:
            return True
        else:
            self.logger.warn("Type %s is not registered to be obtained", tpname)
        return False

    def app_tp_check(self, tpname):
        return self.__type_check(tpname)

    def register_app(self, types_allowed, types_extra):
        self.__app_data = RecursiveDictionary()
        self.__app_allowed_types = types_allowed
        for tpname in types_allowed.union(types_extra):
            self.reset_cache_for_type(tpname)

    def add_new(self, tpname, new):
        if self.app_tp_check(tpname):
            # New has to be update, because it is replacing the object, not merging it.
            self.__app_data[tpname]["new"].update(new)
            


    def add_updated(self, tpname, updated):
        if self.app_tp_check(tpname):
            self.__app_data[tpname]["mod"].rec_update(updated)

    def add_deleted(self, tpname, deleted):
        if self.app_tp_check(tpname):
            self.__app_data[tpname]["deleted"].update(deleted)
            for id in deleted:
                self.remove_id(tpname, id)

    def add(self, tpname, new, updated, deleted):
        self.add_new(tpname, new)
        self.add_updated(tpname, updated)
        self.add_deleted(tpname, deleted)

    def reset_cache_for_type(self, tpname):
        self.__app_data[tpname] = RecursiveDictionary({"new": RecursiveDictionary(), 
                                                                "mod": RecursiveDictionary(), 
                                                                "deleted": set()})

    def reset_tracking_cache_for_type(self, tpname):
        self.__app_data[tpname] = RecursiveDictionary({"new": RecursiveDictionary(), 
                                                                "mod": self.__app_data[tpname]["mod"], 
                                                                "deleted": set()})
    def reset_cache_for_all_types(self):
        for tpname in self.__app_data:
            self.reset_cache_for_type(tpname)

    def reset_tracking_cache_for_all_types(self):
        for tpname in self.__app_data:
            self.reset_tracking_cache_for_type(tpname)

    def get_new(self, tpname):
        return self.__app_data[tpname]["new"] if self.app_tp_check(tpname) else {}

    def get_updated(self, tpname):
        return self.__app_data[tpname]["mod"] if self.app_tp_check(tpname) else {}
        

    def get_deleted(self, tpname):
        return (list(self.__app_data[tpname]["deleted"]) 
                    if self.app_tp_check(tpname) else 
               [])

    def get_all_updates(self, tpname):
        return (self.get_new(tpname),
                self.get_updated(tpname),
                self.get_deleted(tpname))

    def remove_id(self, tpname, id):
        if self.app_tp_check(tpname):
            if id in self.__app_data[tpname]["new"]:
                del self.__app_data[tpname]["new"][id]
            if id in self.__app_data[tpname]["mod"]:
                del self.__app_data[tpname]["mod"][id]
            