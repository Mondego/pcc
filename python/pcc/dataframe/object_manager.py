from multiprocessing import RLock
from recursive_dictionary import RecursiveDictionary
from dataframe_changes.IDataframeChanges import Event, Record

from create import create, change_type

#################################################
#### Object Management Stuff (Atomic Needed) ####
#################################################

    
class ObjectManager(object):
    def __init__(self, calculate_pcc = True):
        self. lock = RLock()
        # <group key> -> id -> object state. (Might have to make this even better)
        # object state is {"base": base state, "type 1": extra fields etc., ...}
        self.current_state = dict()

        self.object_map = dict()

        self.calculate_pcc = calculate_pcc

    #################################################
    ### Static Methods ##############################
    #################################################
    
    @staticmethod
    def __convert_to_dim_map(obj):
        return RecursiveDictionary([(dim, getattr(obj, dim._name)) for dim in obj.__dimensions__ if hasattr(obj, dim._name)])
    
    @staticmethod
    def __make_pcc(pcctype, relevant_objs, param_map, params, hasSingleton = False, hasCollection = False):
        universe = list()
        param_list = list()
        for tp in pcctype.__ENTANGLED_TYPES__:
            universe.append(relevant_objs[tp])

        # Creating random rule for parameters, all Collections first, then singleton objects
        # Why? Random, couldnt think of a better way

        if hasCollection:
            for tp in pcctype_obj.parameter_types[ParameterMode.Collection]:
                param_list.append(param_map[tp])
        if hasSingleton:
            param_list.extend(params)

        try:
            pcc_objects = create(pcctype, *universe, params = param_list)
        except TypeError, e:
            print ("Exception in __make_pcc: " + e.message + " type: " + pcctype.__realname__)
            return list()
        return pcc_objects

    @staticmethod
    def __construct_pccs(pcctype_obj, pccs, universe, param):
        pcctype = pcctype_obj.type
        if pcctype in pccs:
            return pccs[pcctype]
        params, paramtypes = list(), list()
        hasSingleton = False
        hasCollection = False
        if pcctype_obj.has_params:
            if ParameterMode.Singleton in pcctype_obj.parameter_types:
                if param == None or len(pcctype_obj.parameter_types[ParameterMode.Singleton]) != len(param):
                    raise TypeError("Connection construct parameter type %s without right parameters" % pcctype.__realname__)
                params = param
                hasSingleton = True
            if ParameterMode.Collection in pcctype_obj.parameter_types:
                paramtypes = list(pcctype_obj.parameter_types[ParameterMode.Collection])
                hasCollection = True
        
        dependent_types = pcctype_obj.depends
        dependent_pccs = [tp_obj for tp_obj in (dependent_types + paramtypes) if tp_obj not in universe]
        if len([_ for tp_obj in dependent_pccs if tp_obj.is_base_type]) > 0:
            pccs[pcctype] = list()
            return pccs[pcctype]
        to_be_resolved = [tp_obj for tp_obj in dependent_pccs if tp_obj.type not in pccs]
        
        for tp in to_be_resolved:
            dataframe.__construct_pccs(tp_obj, pccs, universe, None)
        param_collection_objs = dict([(tp,
               universe[tp_obj.name].values()
                 if tp_obj.type not in pccs else
               pccs[tp_obj.type]) for tp_obj in paramtypes])

        relevant_objs = dict([(tp_obj.type,
               universe[tp_obj.name].values()
                 if tp_obj.type not in pccs else
               pccs[tp_obj.type]) for tp_obj in dependent_types])

        pccs[pcctype] = dict(((obj.__primarykey__, obj) 
                              for obj in ObjectManager.__make_pcc(
                                            pcctype, 
                                            relevant_objs, 
                                            param_collection_objs, 
                                            params, 
                                            hasSingleton, 
                                            hasCollection)))

    @staticmethod
    def build_pccs(pcctypes_objs, universe, params):
        pccs = dict()
        for pcctype_obj in pcctypes_objs:
            ObjectManager.__construct_pccs(pcctype_obj, pccs, universe, params)
        return pccs

    
    #################################################
    ### API Methods #################################
    #################################################
        
    def create_table(self, tpname, basetype = False):
        with self.lock:
            self.__create_table(tpname, basetype)
        
    def create_tables(self, tpnames_basetype_pairs):
        with self.lock:
            for tpname, basetype in tpnames_basetype_pairs:
                self.__create_table(tpname, basetype)

    def adjust_pcc(self, tp_obj, objs):
        if not self.calculate_pcc:
            return list()
        
        can_be_created_objs = tp_obj.pure_group_members
        old_memberships = dict()
        
        for othertp_obj in can_be_created_objs:
            othertp = othertp_obj.type
            othertpname = othertp_obj.name
            old_set = old_memberships.setdefault(othertp_obj, set())
            if (othertpname in self.object_map):
                old_set.update(set([oid for oid in self.object_map[othertpname] if oid in objs]))

        obj_map = ObjectManager.build_pccs(can_be_created_objs, {tp_obj.group_key: objs}, None)  
        records = list()
        for othertp in obj_map:
            othertpname = othertp.__realname__
            for oid in obj_map[othertp]:
                event = (Event.Modification
                         if othertpname in self.object_map and oid in self.object_map[othertpname] else
                         Event.New)
                self.object_map.setdefault(othertpname, RecursiveDictionary())[oid] = obj_map[othertp][oid]
                obj_changes = ObjectManager.__convert_to_dim_map(obj_map[othertp][oid]) if Event.New else None
                records.append((event, othertpname, oid, obj_changes, ObjectManager.__convert_to_dim_map(obj_map[othertp][oid])))

        for othertp_obj in old_memberships:
            for oid in old_memberships[othertp_obj].difference(set(obj_map[othertp_obj.type])):
                records.append((Event.Delete, othertp_obj.name, oid, None, None))

        return records

    def append(self, tp_obj, obj):
        records = list()
        with self.lock:
            records.extend(self.__append(tp_obj, obj))
            records.extend(self.adjust_pcc(tp_obj, {obj.__primarykey__: obj}))
        return records

    def extend(self, tp_obj, objs):
        records = list()
        obj_map = dict()
        with self.lock:
            for obj in objs:
                records.extend(self.__append(tp_obj, obj))
                obj_map[obj.__primarykey__] = obj
            records.extend(self.adjust_pcc(tp_obj, obj_map))

    def get_one(self, tp_obj, oid, parameter):
        obj_map = self.__get(tp_obj, parameter)
        return obj_map[oid] if oid in obj_map else None

    def get(self, tp_obj, parameter):
        return self.__get(tp_obj, parameter).values()

    def delete(self, tp_obj, obj):
        records = []
        oid = obj.__primarykey__
        if tp_obj.name in self.object_map and oid in self.object_map[tp_obj.name]:
            del self.object_map[tp_obj.name][oid]
            if tp_obj.type == tp_obj.group_type:
                # The object is the group type
                for othertp_obj in tp_obj.pure_group_members:
                    records.extend(self.delete(othertp_obj, obj))

            return [(Event.Delete, tp_obj.name, oid, None, None)]
        return records

    def delete_all(self, tp_obj):
        records = []
        if tp_obj.name in self.object_map and oid in self.object_map[tp_obj.name]:
            for oid in self.object_map[tp_obj.name]:
                records.extend(self.delete(tp_obj, oid))
        return records

    #################################################
    ### Private Methods #############################
    #################################################

    def __create_table(self, tpname, basetype):
        self.object_map.setdefault(tpname, RecursiveDictionary())
        if basetype:
            self.current_state.setdefault(tpname, RecursiveDictionary())

    def __append(self, tp_obj, obj):
        records = list()
        tp = tp_obj.type
        tpname = tp_obj.name
        groupname = tp_obj.group_key
        # all clear to insert.
        try:
            oid = obj.__primarykey__
        except AttributeError:
            setattr(obj, tp.__primarykey__._name, str(uuid4()))
            oid = obj.__primarykey__
        tpname = tp.__realname__
        

        # Store the state in records
        self.current_state[groupname][oid] = RecursiveDictionary(obj.__dict__)

        # Set the object state by reference to the original object's symbol table
        obj.__dict__ = self.current_state[groupname][oid]
        self.object_map.setdefault(tpname, RecursiveDictionary())[oid] = obj
        self.object_map[tpname][oid].__start_tracking__ = True
        obj_changes = ObjectManager.__convert_to_dim_map(obj)
        records.append((Event.New, tpname, oid, obj_changes, obj_changes))
        return records
        
    def __get(self, tp_obj, parameter):
        tp = tp_obj.type
        tpname = tp_obj.name
        with self.lock:
            if tp_obj.is_pure:
                return self.object_map[tpname] if tpname in self.object_map else dict()
            obj_map = ObjectManager.build_pccs([tp_obj], self.object_map, parameter)
            return obj_map[tp] if tp in obj_map else dict()


