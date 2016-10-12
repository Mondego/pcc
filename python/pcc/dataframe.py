'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import spacetime_property
from recursive_dictionary import RecursiveDictionary
from create import create, change_type
from uuid import uuid4
from parameter import ParameterMode
from multiprocessing import RLock

import dataframe_changes.IDataframeChanges as df_repr
from dataframe_changes.IDataframeChanges import Event, Record


BASE_TYPES = set([])

class DataframeModes(object):
    Master = 0
    ApplicationCache = 1
    Client = 2

class ObjectType(object):
    UnknownType = 0
    PCCBase = 1
    ISA = 2
    Subset = 3
    Join = 4
    Projection = 5
    Union = 6
    Param = 7
    Impure = 8

class dataframe(object):
    def __init__(self, lock = RLock(), mode = DataframeModes.Master, name = str(uuid4())):
        self.mode = mode
        # PCCs to be calculated only if it is in Master Mode.
        self.calculate_pcc = (mode == DataframeModes.Master)

        # group key (always string fullanme of type) to members of group.  For example: car -> activecar,
        # inactivecar, redcar, etc etc.
        self.group_to_members = dict()
        
        # str name to class.
        self.name2class = dict()

        # member to groups it belongs to. (should be just one i think)
        # redcar -> car
        self.member_to_group = dict()
        
        # <group key> -> id -> object state. (Might have to make this even better)
        # object state is {"base": base state, "type 1": extra fields etc., ...}
        self.current_state = dict()

        self.object_map = dict()

        # set of types that are impure by default and hence cannot be maintained continuously.
        self.impure = set()

        # dependent type. activecar -depends on-> car
        # pedestrianAndCarNearby -depends on-> activecar, walker, car, pedestrian (goes all the way)
        self.depends_map = dict()

        self.categories = dict()

        self.fake_class_map = dict()

        self.record_map = dict()

        self.start_recording = False

        self.current_buffer = RecursiveDictionary()

        # groupname -> {oid -> proto object representing changes.}
        self.current_record = RecursiveDictionary()

        self.attached_dataframes = set()

        self.tp_to_attached_df = dict()

        self.df_to_tp = dict()

        self.observing_types = set()

        self.known_objects = self.object_map if self.mode != DataframeModes.ApplicationCache else dict()

        self.name = name

        self.lock = lock

        self.temp_record = list()

        self.__join_types = set()

        self.deleted_objs = RecursiveDictionary()
            
        
    def __get_depends(self, tp):
        if hasattr(tp, "__pcc_bases__"):
            return tp.__pcc_bases__
        raise TypeError("Type %s needs to have some bases" % tp.__realname__)

    def __get_group_key(self, tp):
        if tp.__PCC_BASE_TYPE__ or (hasattr(tp, "__pcc_join__") and tp.__pcc_join__):
            return tp.__realname__, tp
        else:
            return self.__get_group_key(list(tp.__pcc_bases__)[0])

    def __check_validity(self, tp, obj):
        if not hasattr(tp, "__realname__"): 
            # Fail to add new obj, because tp was incompatible, or not found.
            raise TypeError("Type %s cannot be inserted/deleted into Dataframe, declare it as pcc_set." % tp.__class__.__name__)
        if not tp.__realname__ in self.observing_types:
            raise TypeError("Type %s cannot be inserted/deleted into Dataframe, register it first." % tp.__realname__) 
        if ObjectType.PCCBase not in self.categories[tp.__realname__]:
            # Person not appending the right type of object
            raise TypeError("Cannot insert/delete type %s" % tp.__realname__)
        if tp.__realname__ != obj.__class__.__realname__:
            raise TypeError("Object type and type given do not match")
        if not hasattr(obj, "__primarykey__"):
            raise TypeError("Object must have a primary key dimension to be used with Dataframes")

    def __convert_to_dim_map(self, obj):
        return dict([(dim, getattr(obj, dim._name)) for dim in obj.__dimensions__ if hasattr(obj, dim._name)])

    
    def __append(self, tp, obj):
        self.__check_validity(tp, obj)

        # all clear to insert.

        if obj.__primarykey__ == None:
            setattr(obj, tp.__primarykey__._name, str(uuid4()))

        oid = obj.__primarykey__
        tpname = tp.__realname__
        groupname = self.member_to_group[tpname]

        # Store the state in records
        self.current_state[groupname][oid] = RecursiveDictionary(obj.__dict__)

        # Set the object state by reference to the original object's symbol table
        obj.__dict__ = self.current_state[groupname][oid]
        obj.__class__ = obj.__class__.Class() if hasattr(obj.__class__, "Class") else obj.__class__
        self.object_map.setdefault(tpname, RecursiveDictionary())[oid] = obj
        self.object_map[tpname][oid].__start_tracking__ = True
        if self.start_recording:
           self.add_to_record_cache(Event.New, tpname, oid, self.__convert_to_dim_map(obj))
        return groupname, oid
    
    def __create_fake_class(self):
        class _container(object):
            @staticmethod
            def add_dims(dims):
                for dim in dims:
                    setattr(_container, dim._name, dim)
        return _container

    def __categorize(self, tp):
        all_categories = set()
        if not (hasattr(tp, "__realname__") or hasattr(tp, "__PCC_BASE_TYPE__")):
            return set([ObjectType.UnknownType])
        
        if tp.__PCC_BASE_TYPE__:
            all_categories.add(ObjectType.PCCBase)
        if hasattr(tp, "__pcc_subset__") and tp.__pcc_subset__:
            all_categories.add(ObjectType.Subset)
        if hasattr(tp, "__pcc_join__") and tp.__pcc_join__:
            all_categories.add(ObjectType.Join)
        if hasattr(tp, "__pcc_projection__") and tp.__pcc_projection__:
            all_categories.add(ObjectType.Projection)
        if hasattr(tp, "__pcc_union__") and tp.__pcc_union__:
            all_categories.add(ObjectType.Union)
        if hasattr(tp, "__pcc_param__") and tp.__pcc_param__:
            all_categories.add(ObjectType.Param)
            all_categories.add(ObjectType.Impure)
        if hasattr(tp, "__pcc_isa__") and tp.__pcc_isa__:
            all_categories.add(ObjectType.ISA)
        if hasattr(tp, "__pcc_impure__") and tp.__pcc_impure__:
            all_categories.add(ObjectType.Impure)

        return all_categories

    def __change_type(obj, totype):
        class container(totype):
            __metaclass__ = PCCMeta(totype)
            __original_class__ = totype
            def __init__(self):
                pass

        new_obj = container()
        new_obj.__dict__ = obj.__dict__
        return new_obj

    def __is_impure(self, tp, categories):
        if self.mode == DataframeModes.Client:
            return False
        return (len(set([ObjectType.Join,
                         ObjectType.Impure,
                         ObjectType.Param,
                         ObjectType.UnknownType]).intersection(
                             categories)) > 0)

    def __record_pcc_changes(self, new_objs_map, old_memberships, original_changes = None):
        '''
        This function records the changes (modifications, new, deletes) in the objects found 
        in new_objs_map. The reference to record these changes is the old_memberships map.
        original_changes is a record of the dimension changes that occured to incur this pcc change
        record
        '''
        id_map = dict()

        for othertp, new_objs in new_objs_map.items():
            othertpname = othertp.__realname__
            id_map[othertp] = set()
            for new_obj in new_objs:
                try:
                    # Adding the object into the id_map for recording.
                    id_map[othertp].add(new_obj.__primarykey__)
                except AttributeError:
                    # It's a join, no ID in join. :|
                    new_obj.__primarykey__ = str(uuid4()) # This should set it to new UUID
                if new_obj.__primarykey__ not in old_memberships[othertp]:
                    # adding this object as a new entry to type othertp
                    if self.start_recording or othertpname in self.tp_to_attached_df:
                        self.add_to_record_cache(Event.New, othertpname, new_obj.__primarykey__, self.__convert_to_dim_map(new_obj)) 
                else:
                    # This object was already a member of othertp. Keeping this as a modification.
                    # The dim changes should have already reached, If not: original_changes will have it.
                    if self.start_recording or othertpname in self.tp_to_attached_df:
                        dims = (original_changes[self.member_to_group[othertpname]][new_obj.__primarykey__]["dims"] 
                                if original_changes and othertpname in self.member_to_group and self.member_to_group[othertpname] in original_changes else
                                None)
                        self.add_to_record_cache(Event.Modification, othertpname, new_obj.__primarykey__, dims, already_converted = True) 
                self.object_map.setdefault(
                    othertpname, 
                    RecursiveDictionary())[new_obj.__primarykey__] = new_obj
                new_obj.__start_tracking__ = True

        for othertp in old_memberships:
            # Finding all the objects that were removed from the type in this operation.
            othertpname = othertp.__realname__
            for oid in set(old_memberships[othertp]).difference(set(id_map[othertp])):
                # all oid's that were in the old set for the type and not there in the new set for the type.
                if self.start_recording or othertpname in self.tp_to_attached_df:
                    self.add_to_record_cache(Event.Delete, othertpname, oid, None)
                del self.object_map[othertpname][oid]

        return new_objs_map

    def __adjust_pcc(self, objs, groupname, original_changes = None):
        if not self.calculate_pcc:
            return
        can_be_created = list()
        for othertp in self.group_to_members[groupname]:
            other_cats = self.categories[othertp.__realname__]
            if ((not self.__is_impure(othertp, other_cats))
                and len(set([ObjectType.Projection,
                             ObjectType.Subset,
                             ObjectType.Union]).intersection(
                                 other_cats)) > 0):
                can_be_created.append(othertp)

        old_memberships = dict()
        for othertp in can_be_created:
            othertpname = othertp.__realname__
            old_set = old_memberships.setdefault(othertp, set())
            if (othertpname in self.known_objects):
                old_set.update(set([oid for oid in self.known_objects[othertpname] if oid in objs]))

        obj_map = self.__calculate_pcc(can_be_created, {groupname: objs}, None)  
        new_objs_map = self.__record_pcc_changes(obj_map,
                                                 old_memberships,
                                                 {groupname: original_changes} if original_changes else None)
        for othertp, new_objs in new_objs_map.items():
            for new_obj in new_objs:
                self.object_map.setdefault(
                    othertp.__realname__, 
                    RecursiveDictionary())[new_obj.__primarykey__] = new_obj
                new_obj.__start_tracking__ = True

    def __make_pcc(self, pcctype, relevant_objs, param_map, params, hasSingleton = False, hasCollection = False):
        universe = list()
        param_list = list()
        for tp in pcctype.__ENTANGLED_TYPES__:
            universe.append(relevant_objs[tp])

        # Creating random rule for parameters, all Collections first, then singleton objects
        # Why? Random, couldnt think of a better way

        if hasCollection:
            for tp in pcctype.__parameter_types__[ParameterMode.Collection]:
                param_list.append(param_map[tp])
        if hasSingleton:
            param_list.extend(params)

        try:
            pcc_objects = create(pcctype, *universe, params = param_list)
        except TypeError, e:
            print ("Exception in __make_pcc: " + e.message)
            return list()
        return pcc_objects

    def __construct_pccs(self, pcctype, pccs, universe, param):
        if pcctype in pccs:
            return pccs[pcctype]
        params, paramtypes = list(), list()
        hasSingleton = False
        hasCollection = False
        if hasattr(pcctype, "__parameter_types__"):
            if ParameterMode.Singleton in pcctype.__parameter_types__:
                if param == None or len(pcctype.__parameter_types__[ParameterMode.Singleton]) != len(param):
                    raise TypeError("Connection construct parameter type %s without right parameters" % pcctype.__realname__)
                params = param
                hasSingleton = True
            if ParameterMode.Collection in pcctype.__parameter_types__:
                paramtypes = list(pcctype.__parameter_types__[ParameterMode.Collection])
                hasCollection = True
        
        dependent_types = list(pcctype.__ENTANGLED_TYPES__)
        dependent_pccs = [tp for tp in (dependent_types + paramtypes) if tp.__realname__ in self.impure or tp.__realname__ not in universe]
        if len([tp for tp in dependent_pccs if tp.__PCC_BASE_TYPE__]) > 0:
            pccs[pcctype] = list()
            return pccs[pcctype]
        to_be_resolved = [tp for tp in dependent_pccs if tp not in pccs]
        
        for tp in to_be_resolved:
            self.__construct_pccs(tp, pccs, universe, None)
        param_collection_objs = dict([(tp,
               universe[tp.__realname__].values()
                 if tp not in pccs else
               pccs[tp]) for tp in paramtypes])

        relevant_objs = dict([(tp,
               universe[tp.__realname__].values()
                 if tp not in pccs else
               pccs[tp]) for tp in dependent_types])

        pccs[pcctype] = self.__make_pcc(pcctype, relevant_objs, param_collection_objs, params, hasSingleton, hasCollection)

    def __calculate_pcc(self, pcctypes, universe, params, force = False):
        if not (self.calculate_pcc or force):
           return dict() 
        pccs = dict()
        for pcctype in pcctypes:
            self.__construct_pccs(pcctype, pccs, universe, params)
        return pccs

    def __process_record(self, record):
        if record["type"] == Record.INT:
            # the value will be in record["value"]
            return long(record["value"])
        if record["type"] == Record.FLOAT:
            # the value will be in record["value"]
            return float(record["value"])
        if record["type"] == Record.STRING:
            # the value will be in record["value"]
            return record["value"]
        if record["type"] == Record.BOOL:
            # the value will be in record["value"]
            return record["value"]
        if record["type"] == Record.NULL:
            # No value, just make it None
            return None
            
        if record["type"] == Record.OBJECT:
            # The value is {
            #    "omap": <Dictionary Record form of the object (__dict__)>,
            #    "type": {"name": <name of type, "type_pickled": pickled string of type <- optional part
            #  }

            # So parse it like a dict and update the object dict
            new_record = RecursiveDictionary()
            new_record["type"] = Record.DICTIONARY
            new_record["value"] = record["value"]["omap"]
                
            dict_value = self.__process_record(new_record)
            value = self.__create_fake_class()()
            # Set type of object from record.value.object.type. Future work.
            value.__dict__ = dict_value
            return value
        if record["type"] == Record.COLLECTION:
            # Assume it is list, as again, don't know this type
            # value is just list of records
            return [self.__process_record(rec) for rec in record["value"]]
        if record["type"] == Record.DICTIONARY:
            # Assume it is dictionary, as again, don't know this type
            # value-> [{"k": key_record, "v": val_record}] Has to be a list because key's may not be string
            return RecursiveDictionary([
                    (self.__process_record(p["k"]), self.__process_record(p["v"])) 
                    for p in record["value"]])
        if record["type"] == Record.FOREIGN_KEY:
            # value -> {"group_key": group key, 
            #           "actual_type": {"name": type name, "type_pickled": pickle form of type}, 
            #           "object_key": object key}
            groupname = record["value"]["group_key"]
            oid = record["value"]["object_key"]
            if groupname not in self.group_to_members:
                # This type cannot be created, it is not registered with the DataframeModes
                return None
            actual_type_name = (record["value"]["actual_type"]["name"]
                                 if "actual_type" in record["value"] and "name" in record["value"]["actual_type"] else
                                groupname)
            actual_type_name, actual_type = (
                            (actual_type_name, self.name2class[actual_type_name])
                              if (actual_type_name in self.name2class) else
                            (groupname, self.name2class[groupname]))
            
            if groupname in self.current_state and oid in self.current_state[groupname]:
            # The object exists in one form or the other.
                if actual_type_name in self.object_map and oid in self.object_map[actual_type_name]:
                    # If the object already exists. Any new object will update that.
                    return self.object_map[actual_type_name][oid]
                # The group object exists, but not in the actual_type obj.
            # The object does not exist, create a dummy one and the actual object will get updated 
            # in some other group change in this iteration.
            obj_state =  self.current_state.setdefault(groupname, RecursiveDictionary()).setdefault(oid, RecursiveDictionary())
            obj = self.__create_fake_class()()
            obj.__dict__ = obj_state
            obj.__class__ = actual_type
            self.object_map.setdefault(actual_type_name, RecursiveDictionary())[oid] = obj
            return obj 
            

        raise TypeError("Do not know dimension type %s", record["type"])

    def __build_dimension_obj(self, dim_received, groupname):
        dim_map = RecursiveDictionary()
        fakeclass = self.fake_class_map[groupname]
        obj = fakeclass()
        for dim in dim_received:
            record = dim_received[dim]
            dim_map[dim] = record
            if not hasattr(fakeclass, dim):
                continue
            if record["type"] == Record.OBJECT:
                new_record = RecursiveDictionary()
                new_record["type"] = Record.DICTIONARY
                new_record["value"] = record["value"]["omap"]
                dict_value = self.__process_record(new_record)
                value = self.__create_fake_class()()
                value.__dict__ = dict_value
                value.__class__ = getattr(fakeclass, dim)._type
            elif (record["type"] == Record.COLLECTION 
                or record["type"] == Record.DICTIONARY):
                collect = self.__process_record(record)
                value = getattr(fakeclass, dim)._type(collect)
            else:    
                value = self.__process_record(record)
            setattr(obj, dim, value)
        return obj, dim_map

    def __get_obj_type(self, obj):
        # both iteratable/dictionary + object type is messed up. Won't work.
        try:
            if obj.__class__.__name__ in self.member_to_group:
                return Record.FOREIGN_KEY
            if dict in type(obj).mro():
                return Record.DICTIONARY
            if hasattr(obj, "__iter__"):
                #print obj
                return Record.COLLECTION
            obj_mro = set(type(obj).mro())
            if int in obj_mro or long in obj_mro:
                return Record.INT
            if float in obj_mro:
                return Record.FLOAT
            if str in obj_mro or unicode in obj_mro:
                return Record.STRING
            if bool in obj_mro:
                return Record.BOOL
            if type(None) in obj_mro:
                return Record.NULL
            if hasattr(obj, "__dict__"):
                return Record.OBJECT
        except TypeError, e:
            return -1
        return -1

    def __generate_dim(self, dim_change, foreign_keys):
        dim_type = self.__get_obj_type(dim_change)
        dim = RecursiveDictionary()
        dim["type"] = dim_type
        if dim_type == Record.INT:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.FLOAT:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.STRING:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.BOOL:
            dim["value"] = dim_change
            return dim
        if dim_type == Record.NULL:
            return dim
            
        if dim_type == Record.COLLECTION:
            dim["value"] = [self.__generate_dim(v, foreign_keys) for v in dim_change]
            return dim                
        
        if dim_type == Record.DICTIONARY:
            dim["value"] = [{"k": self.__generate_dim(k, foreign_keys), 
                             "v": self.__generate_dim(v, foreign_keys)} 
                            for k, v in dim_change.items()]
            return dim
            
        if dim_type == Record.OBJECT:
            dim["value"] = RecursiveDictionary()
            dim["value"]["omap"] = (
                self.__generate_dim(dim_change.__dict__, foreign_keys)["value"])
            # Can also set the type of the object here serialized. Future work. 
            return dim

        if dim_type == Record.FOREIGN_KEY:
            key, group, fk_type = dim_change.__primarykey__, self.member_to_group[dim_change.__class__.__name__], dim_change.__class__.__name__
            foreign_keys.append((key, fk_type, group))
            dim["value"] = RecursiveDictionary()
            dim["value"]["group_key"] = group
            dim["value"]["object_key"] = key
            dim["value"]["actual_type"] = RecursiveDictionary()
            dim["value"]["actual_type"]["name"] = fk_type
            return dim

        raise TypeError("Don't know how to deal with %s" % dim_change)

    def __report_dim_modification(self, oid, name, value, groupname):
        if groupname in self.name2class and self.name2class[groupname] in self.group_to_members[groupname]:
            self.add_to_record_cache(Event.Modification, groupname, oid, {name: value}) 
            self.apply_records_in_cache()

    def connect(self, new_df):
        # If I connect the current states, the __dict__ of objects in the two dataframes will be in reference.
        for tpname in new_df.observing_types:
            self.tp_to_attached_df.setdefault(tpname, set()).add(new_df)
        self.df_to_tp[new_df] = new_df.observing_types
        self.attached_dataframes.add(new_df)
        new_df.current_state = self.current_state
        new_df.object_map = self.object_map
        new_df.record_using_objmap(new_df.object_map)

    def add_type(self, tp, except_type = None, tracking=False, not_member=False):
        with self.lock:
            self.__add_type(tp, except_type, tracking, not_member)

    def __add_type(self, tp, except_type = None, tracking=False, not_member=False):
        categories = self.__categorize(tp)
        # str name of the type.
        name = tp.__realname__
        
        if ObjectType.UnknownType in categories:
            raise TypeError("Type %s cannot be added" % name)
        
        # add type to map
        self.name2class[name] = tp

        if not not_member:
            self.observing_types.add(name)

        # getting all the dependencies that tp depends on (results are in string form)
        depend_types = self.__get_depends(tp)
        self.depends_map[name] = depend_types

        for dtp in depend_types:
            dtpname = dtp.__realname__
            if dtpname == except_type:
                raise TypeError("Cyclic reference detected in definition of %s" % name)
            if dtpname not in self.name2class:
                self.__add_type(dtp, except_type = name, not_member=True)
            if ObjectType.Impure in self.categories[dtpname]:
                categories.add(ObjectType.Impure)

        # add categories to map
        self.categories[name] = categories
        
        if ObjectType.Join in categories or ObjectType.Impure in categories:
            self.impure.add(name)
        
        # Type isnt impure. things are getting interesting. I can cache it!
        # str key for the group it belongs to.
        key, keytp = self.__get_group_key(tp)
        self.name2class[key] = keytp
    
        # Adding name to the group
        self.group_to_members.setdefault(key, set()).add(tp)
        self.member_to_group[name] = key
        self.current_state.setdefault(key, RecursiveDictionary())
        self.fake_class_map.setdefault(key, self.__create_fake_class()).add_dims(tp.__dimensions__)
        
        if ObjectType.Join in categories:
            self.__join_types.add(name)

        for dim in tp.__dimensions__:
            dim._dataframe_data.add((key, self.__adjust_pcc, self.__report_dim_modification))

    def add_types(self, types, tracking=False):
        with self.lock:
            for tp in types:
                self.__add_type(tp)

    def remove_type(self, tp):
        with self.lock:
            tpname = tp.__realname__
            if tpname in self.name2class:
                gp = self.member_to_group[tpname]
                if gp == tpname and len(self.group_to_members[gp]) > 1:
                    raise TypeError("Cannot delete %s without removing all subtypes of it first" % tpname)
                del self.member_to_group[tpname]
                self.group_to_members[gp].remove(tp)
                if tpname in self.object_map:
                    del self.object_map[tpname]
                del self.name2class[tpname]
                del self.categories[tpname]
                if tpname in self.impure:
                    self.impure.remove(tpname)
            else:
                raise TypeError("Type %s is not in the dataframe." % tpname)

    def append(self, tp, obj):
        with self.lock:
            groupname, oid = self.__append(tp, obj)
        self.__adjust_pcc({oid, obj}, groupname)
        self.apply_records_in_cache()
        
    def extend(self, tp, objs):
        
        objmap = dict()
        with self.lock:
            for obj in objs:
                groupname, oid = self.__append(tp, obj)
                objmap[oid] = obj
        if len(objmap) == 0:
            return
        self.__adjust_pcc(objmap, groupname)
        self.apply_records_in_cache()
    
    def get(self, tp, parameters=None):

        tpname = tp.__realname__
        if tpname not in self.observing_types:
            raise TypeError("Type %s not registered" % tpname)
        
        with self.lock:
            if not self.__is_impure(tp, self.categories[tp.__realname__]) and tpname in self.object_map:
                return self.object_map[tpname].values()

        if hasattr(tp, "__PCC_BASE_TYPE__") and tp.__PCC_BASE_TYPE__:
            return list()

        if not self.calculate_pcc:
            # Not calculating pcc if Client/Cache mode
            return list()
        groupkey = self.member_to_group[tpname]
        objs = list()
        with self.lock:
            objs = self.__calculate_pcc([tp], self.object_map, parameters)
        return_values = self.__record_pcc_changes(objs, dict().fromkeys(objs, list()))[tp]
        self.apply_records_in_cache()
        return return_values

    def delete(self, tp, obj):
        with self.lock:
            self.__check_validity(tp, obj)
            # all clear to delete
            oid = obj.__primarykey__
            tpname = tp.__realname__
            groupname = self.member_to_group[tpname]
            del self.current_state[groupname][oid]
            self.deleted_objs.setdefault(groupname, set()).add(oid)
            # delete from object_map too
            deleted_members = list()
            for member in self.group_to_members[groupname]:
                if member.__realname__ in self.object_map and oid in self.object_map[member.__realname__]:
                    del self.object_map[member.__realname__][oid]
                    deleted_members.append(member.__realname__)
        # Doing it outside, to precent deadlock.
        for mem in deleted_members:
            self.add_to_record_cache(Event.Delete, mem, oid)
        self.add_to_record_cache(Event.Delete, tpname, oid)
        self.apply_records_in_cache()

    def remove_types(self, types):
        for tp in types:
            self.remove_type(tp)

    def reload_types(self, types):
        # maybe clear maps and data? before reloading. Will see later.
        with self.lock:
            self.add_types(types)
    
    def __create_objs(self, all_changes):
        '''
        all_changes is a dictionary in this format
        {
            "gc": { <- gc stands for group changes
                "group_key1": { <- Group key for the type EG: Car
                    "object_key1": { <- Primary key of object
                        "dims": { <- optional
                            "dim_name": { <- EG "velocity"
                                "type": <Record type, EG Record.INT. Enum values can be found in Record class>
                                "value": <Corresponding value, either a literal, or a collection, or a foreign key format. Can be optional i type is Null
                            },
                            <more dim records> ...
                        },
                        "types": {
                            "type_name": <status of type. Enum values can be found in Event class>,
                            <More type to status mappings>...
                        }
                    },
                    <More object change logs> ...
                },
                <More group change logs> ...
            },
            "types": [ <- A list of pickled types bein sent for object conversion. Not used atm.
                {
                    "name": <name of the type>,
                    "type_pickled": <pickle string of the type class>
                },
                <More type records> ...
            ]
        }
        '''
        current_obj_map = RecursiveDictionary()
        part_obj_map = RecursiveDictionary()
        group_changes_json = RecursiveDictionary()
        deletes = dict()

        # For each group
        for groupname, groupchanges in all_changes["gc"].items():
            # For each object in each group
            for oid, obj_changes in groupchanges.items():
                if groupname in self.deleted_objs and oid in self.deleted_objs:
                    continue
                
                final_objjson = RecursiveDictionary()
                new_obj = None
                dim_map = RecursiveDictionary()

                # If there are dimension changes to pick up
                if "dims" in obj_changes and len(obj_changes["dims"]) > 0:
                    new_obj, dim_map = self.__build_dimension_obj(
                        obj_changes["dims"], 
                        groupname)
                    if oid in self.current_state[groupname]:
                        # getting actual reference if it is there.
                        final_objjson = self.current_state[groupname][oid]
                    # Updatin the object json with the new objects state
                    final_objjson.rec_update(new_obj.__dict__)
                    # Storing that state
                    self.current_state[groupname][oid] = final_objjson
                    # Connecting the object to the state
                    new_obj.__dict__ = final_objjson
                    # Addin the object to the part map
                    part_obj_map.setdefault(groupname, dict())[oid] = new_obj
                obj_change_json = group_changes_json.setdefault(groupname, RecursiveDictionary()).setdefault(oid, RecursiveDictionary())
                if dim_map:
                    obj_change_json["dims"] = dim_map 
                # For all type and status changes for that object
                for member, status in obj_changes["types"].items():
                    obj_change_json.setdefault("types", RecursiveDictionary())[member] = status
                    # If member is not tracked by the dataframe
                    if not (member in self.member_to_group and self.member_to_group[member] == groupname and member in self.observing_types):
                        continue
                    # If the object is New, or New for this dataframe.
                    if status == Event.New or (status == Event.Modification and (member not in self.known_objects or oid not in self.known_objects[member])):
                        # setting a new object of the required type into object_map so that it can be pulled with get and other such functions
                        self.object_map.setdefault(member, RecursiveDictionary())[oid] = change_type(new_obj, self.name2class[member])
                        # Allow changes to the object to be tracked by the dataframe
                        self.object_map[member][oid].__start_tracking__ = True
                        # Marking this object as a new for get_new dataframe call
                        self.add_to_buffer(Event.New, member, oid)
                    # If this dataframe knows this object
                    elif status == Event.Modification:
                        # Markin this object as a modified object for get_mod dataframe call.
                        # Changes to the base object would have already been applied, or will be applied goin forward.
                        self.add_to_buffer(Event.Modification, member, oid)
                        # Should get updated through current_state update when current_state changed.
                    # If the object is being deleted.
                    elif status == Event.Delete:
                        if member in self.known_objects and oid in self.known_objects[member]:
                            # Addin the object to the get_deleted buffer. It is not tracked by the dataframe any more.
                            self.add_to_buffer(Event.Delete, member, oid)
                            # Maintaining a list of deletes for seein membership changes later.
                            deletes.setdefault(member, set()).add(oid)
                    else:
                        raise Exception("Object change Status %s unknown" % status)

        # Returns deleted objs for each type, 
        # an object map that can be merged with the actual object map, 
        # a list of changes that were apply for record keeping        
        return deletes, part_obj_map, group_changes_json

    def __apply(self, df_changes):
        # See __create_objs to see the format of df_changes
        relevant_changes = dict()
        part_obj_map = dict()
        group_changes_json = RecursiveDictionary()
        relevant_changes = df_repr.DataframeChanges_Base()
        relevant_changes["gc"] = RecursiveDictionary()
        for groupname, grp_changes in df_changes["gc"].items():
            # if the groupname is relevant.
            if groupname in self.group_to_members:
                relevant_changes["gc"][groupname] = grp_changes

        if "types" in df_changes:
            relevant_changes["types"] = df_changes["types"]
        #Objects are being created and applied to the object_map
        deletes, part_obj_map, group_changes_json = self.__create_objs(relevant_changes)
        
        group_id_map = dict()
        remaining = dict()
        deleted = dict()
        # Doing garbage collection on deleted objects
        for tpname in deletes:
            # If the type is a base type. (Clean up all objects mode)
            if tpname in self.group_to_members:
                # Find memberships in other tp
                othertps = [t.__realname__ 
                            for t in self.group_to_members[tpname] 
                            if t.__realname__ in deletes and t.__realname__ != tpname]
                # Purge the object
                for oid in deletes[tpname]:
                    del self.object_map[tpname][oid]
                    deleted.setdefault(tpname, set()).add(oid)
                    for othertpname in othertps:
                        if oid in deletes[othertpname]:
                            del self.object_map[othertpname][oid]
                            deleted.setdefault(othertpname, set()).add(oid)
                    self.add_to_record_cache(Event.Delete, tpname, oid)
                    del self.current_state[tpname][oid]
            else:
                # It is just a normal membership change. Do not ndelete the base object.
                for oid in deletes[tpname]:
                    if tpname in deleted and oid in deleted[tpname]:
                        continue
                    remaining.setdefault(tpname, set()).add(oid)
        # If all memberships relevant to the object are deleted, delete the actual object as well.
        for tpname in remaining:
            for oid in remaining[tpname]:
                del self.object_map[tpname][oid]
                group_id_map.setdefault(self.member_to_group[tpname], dict()).setdefault(oid, set()).add(tpname)
                self.add_to_record_cache(Event.Delete, tpname, oid)
        for gname in group_id_map:
            gcount = len(self.group_to_members[gname])
            for oid in group_id_map[gname]:
                if gcount == len(group_id_map[gname][oid]):
                    del self.object_map[gname][oid]
                    del self.current_state[gname][oid]
                    self.add_to_record_cache(Event.Delete, gname, oid)
        return part_obj_map, group_changes_json

    def __report_to_dataframes(self, df_changes, except_df):
        possible_dfs = set()
        for tpname, grpchanges in df_changes["gc"].items():
            # If type is not relevant to any dataframe
            if tpname not in self.tp_to_attached_df:
                continue
            possible_dfs.update(self.tp_to_attached_df[tpname])
            if except_df in possible_dfs:
                possible_dfs.remove(except_df)
                
        # Only send changes to the dataframes potentially interested in these changes.
        for df in possible_dfs:
            df.apply_all(df_changes)

    def __convert_to_serializable_dict(self, current_record):
        df_changes = df_repr.DataframeChanges_Base()
        df_changes.ParseFromDict({"gc": self.current_record})
        return df_changes

    def add_to_record_cache(self, event_type, tpname, oid, dim_change = None, already_converted = False):
        self.temp_record.append((event_type, tpname, oid, dim_change, already_converted))

    def clear_record_cache(self):
        self.temp_record = list()

    def apply_records_in_cache(self):
        for event_type, tpname, oid, dim_change, already_converted in self.temp_record:
            self.record(event_type, tpname, oid, dim_change, already_converted)
        self.clear_record_cache()

    def apply_all(self, df_changes, except_df = None):
        # see __create_objs function for the format of df_changes
        if self.mode == DataframeModes.Master:
            # if master: send changes to all other dataframes attached.
            # apply changes to object_map, and currect_state
            # adjust pcc
            with self.lock:
                # Apply the changes
                objmaps, grp_changes = self.__apply(df_changes)
            # Apply any record changes
            self.apply_records_in_cache()
            # Report base changes to all dataframes interested in them.
            self.__report_to_dataframes(df_changes, except_df)
            # Start adjusting pcc changes for theese changes. only
            for groupname, grpobjs in objmaps.items():
                self.__adjust_pcc(grpobjs, groupname, grp_changes[groupname] if groupname in grp_changes else None)
        elif self.mode == DataframeModes.ApplicationCache:
            # if cache: 
            # forward relevant changes to record
            if not self.start_recording:
                return
            for groupname, groupchanges in df_changes["gc"].items():
                # if the groupname is relevant
                if groupname in self.group_to_members:
                    # for each object in the group
                    for oid, objchanges in groupchanges.items():

                        # This tp -> status map contains only tps that this dataframe is interested in.
                        finaltpmap = RecursiveDictionary(
                            [
                                (tpname, status) 
                                for tpname, status in objchanges["types"].items() 
                                if (tpname in self.member_to_group 
                                    and self.member_to_group[tpname] == groupname 
                                    and tpname in self.observing_types)
                            ]
                        )
                        if len(finaltpmap) > 0:
                            # There are records to process
                            if "dims" in objchanges and len(objchanges["dims"]) > 0:
                                # Pick out only the relevant dimensions for that dataframe type.
                                # This prevents sending useless dimensions for an object type to a dataframe.
                                # For example. If a dataframe subscribes to only a projection of Car, it should
                                # not get all the dimensions for Car on an update, just the ones that are in the projection.
                                dims_relevant = RecursiveDictionary(
                                    [
                                        (dim, value) 
                                        for dim, value in objchanges["dims"].items()
                                        if hasattr(self.fake_class_map[groupname], dim)
                                    ]
                                )
                                # Add it to the current record.
                                self.current_record.setdefault(
                                    groupname, RecursiveDictionary()).setdefault(
                                        oid, RecursiveDictionary()).rec_update(
                                            RecursiveDictionary({"types": finaltpmap, "dims": dims_relevant}))
            return

        elif self.mode == DataframeModes.Client:
            # if master
            # apply changes to object_map, and currect_state
            # Shouldnt need a lock.
            with self.lock:
                self.__apply(df_changes)
        else:
            raise TypeError("Unknown dataframe mode %s" % self.mode)
        self.apply_records_in_cache()
        return

    def record_using_json(self, tpname, oid, changes):
        self.apply_all({tpname: {oid: changes}}, True)
    
    def record_using_objmap(self, objmap):
        for tpname in objmap:
            if tpname not in self.observing_types:
                continue
            for oid in objmap[tpname]:
                self.add_to_record_cache(Event.New, tpname, oid, self.__convert_to_dim_map(objmap[tpname][oid]))     

    def record(self, event_type, tpname, oid, dim_change = None, already_converted = False):
        with self.lock:
            groupname = self.member_to_group[tpname]
            if event_type == Event.Delete and tpname == self.member_to_group[tpname]:
                # it is its own key. Which means the obj is being deleted for good.
                # Purge all changes.
                if groupname in self.current_record and oid in self.current_record[groupname]:
                    if "dims" in self.current_record[groupname][oid]:
                        del self.current_record[groupname][oid]["dims"]
                    for tp in self.current_record[groupname][oid]["types"]:
                        self.current_record[groupname][oid]["types"][tp] = Event.Delete
                self.deleted_objs.setdefault(groupname, set()).add(oid)

                
        if self.start_recording:
            if event_type != Event.Delete and tpname in self.deleted_objs and oid in self.deleted_objs[tpname]:
                # This object is flagged for deletion. Throw this change away.
                return
            with self.lock:
                self.current_record.setdefault(
                    groupname, RecursiveDictionary()).setdefault(
                        oid, RecursiveDictionary({"types": RecursiveDictionary()}))["types"].rec_update(RecursiveDictionary({tpname: event_type}))
                if dim_change:
                    fks = []
                    self.current_record[groupname][oid].setdefault(
                        "dims", RecursiveDictionary()).rec_update(dict([(k if already_converted else k._name, 
                                                                         v if already_converted else self.__generate_dim(v, fks)) 
                                                                        for k, v in dim_change.items()]))
                    for fk, fk_type, group in fks:
                        fk_event_type = Event.Modification if group in self.known_objects and fk in self.known_objects[group] else Event.New
                        fk_dims = None
                        if fk_event_type == Event.New and group in self.object_map and fk in self.object_map[group]:
                            fk_dims = self.__convert_to_dim_map(self.object_map[group][fk])
                        self.record(fk_event_type, fk_type, fk, fk_dims)

        if tpname in self.tp_to_attached_df:
            for df in self.tp_to_attached_df[tpname]:
                if df.start_recording:
                    if event_type == Event.Modification:
                        if tpname not in df.known_objects or (tpname in df.known_objects and oid not in df.known_objects[tpname]):
                            df.record(Event.New, tpname, oid, 
                                      self.__convert_to_dim_map(self.object_map[tpname][oid]))
                            continue
                    df.record(event_type, tpname, oid, dim_change, already_converted)
                    

    def get_record(self, parameters = None):
        # calculate impure at the last minute
        if self.mode != DataframeModes.Client:
            for tpname in self.impure:
                tp = self.name2class[tpname]
                with self.lock:
                    objs = self.__calculate_pcc([tp], self.object_map, parameters, True)
                    known_members = dict([(tp, self.known_objects[tp.__realname__] if tp.__realname__ in self.known_objects else list()) for tp in objs])
                self.__record_pcc_changes(objs, known_members)
                self.apply_records_in_cache()
        
        return self.__convert_to_serializable_dict(self.current_record)

    def clear_record(self):
        if self.mode == DataframeModes.ApplicationCache:
            # Relavent only for application Cache
            with self.lock:
               for tpname, tpitems in self.current_record.items():
                    for oid, objchanges in tpitems.items():
                        for tp, change in objchanges["types"].items():
                            if change == Event.Delete:
                                if tp in self.known_objects and oid in self.known_objects[tp]:
                                    # If it was false, obj was deleted before it was pulled by the other app.
                                    self.known_objects[tp].remove(oid)
                            elif change == Event.New:
                                self.known_objects.setdefault(tp, set()).add(oid)

        self.current_record = RecursiveDictionary()

    def add_to_buffer(self, event_type, tpname, oid):
        if event_type == Event.Delete and tpname == self.member_to_group[tpname]:
            # Preserve the privacy of the deleted obj. It's off the grid.
            self.object_map[tpname][oid].__start_tracking__ = False
        self.current_buffer.setdefault(tpname, RecursiveDictionary({
            Event.New: dict(),
            Event.Modification: dict(),
            Event.Delete: dict()
            }))[event_type].update({oid: self.object_map[tpname][oid]})

    def clear_buffer(self):
        self.current_buffer = RecursiveDictionary()
         
    def clear_all(self):
        self.clear_buffer()
        self.clear_record()
        for tp in self.current_state:
            self.current_state[tp] = RecursiveDictionary()
        for tp in self.object_map:
            self.object_map[tp] = RecursiveDictionary()                                                                               

    def get_new(self, tp):
        tpname = tp.__realname__
        return self.current_buffer[tpname][Event.New].values() if tpname in self.current_buffer else list()

    def get_mod(self, tp):
        tpname = tp.__realname__
        return self.current_buffer[tpname][Event.Modification].values() if tpname in self.current_buffer else list()

    def get_deleted(self, tp):
        tpname = tp.__realname__
        return self.current_buffer[tpname][Event.Delete].values() if tpname in self.current_buffer else list()

    def clear_join_objects(self):
        for tpname in self.__join_types:
            if tpname in self.object_map:
                self.object_map[tpname].clear()
            if tpname in self.known_objects:
                self.known_objects[tpname].clear()

    def serialize_all(self):
        new_record = {}
        with self.lock:
            old_state = self.start_recording
            old_current_record = self.get_record()
            self.clear_record()
            self.start_recording = True
            for tpname in self.object_map:
                for oid in self.object_map[tpname]:
                    self.record(Event.New, tpname, oid, self.__convert_to_dim_map(self.object_map[tpname][oid]))
            self.apply_records_in_cache()
            new_record = self.get_record()
            self.current_record = old_current_record
            self.start_recording = old_state

        return new_record