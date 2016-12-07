from multiprocessing import RLock, Process
from recursive_dictionary import RecursiveDictionary

import dataframe_changes.IDataframeChanges as df_repr
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

        self.deleted_objs = RecursiveDictionary()

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

    def apply_changes(self, df_changes, name2type, except_df = None):
        # see __create_objs function for the format of df_changes
        # if master: send changes to all other dataframes attached.
        # apply changes to object_map, and currect_state
        # adjust pcc
        with self.lock:
            # Apply the changes
            objmaps, grp_changes, records, buffer_changes = self.__apply(df_changes, name2type)
        # Start adjusting pcc changes for theese changes. only
        for groupname, grpobjs in objmaps.items():
            records.extend(self.adjust_pcc(name2type[groupname], grpobjs))
        return records, buffer_changes

    

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

    def __apply(self, df_changes, name2type):
        # See __create_objs to see the format of df_changes
        part_obj_map = dict()
        group_changes_json = RecursiveDictionary()
        #Objects are being created and applied to the object_map
        deletes, part_obj_map, group_changes_json, buffer_changes = self.__create_objs(df_changes, name2type)
        
        group_id_map = dict()
        remaining = dict()
        deleted = dict()
        records = list()
        # Doing garbage collection on deleted objects
        for tp_obj in deletes:
            tpname = tp_obj.name
            # If the type is a base type. (Clean up all objects mode)
            if tp_obj.group_type == tp_obj.type:
                # Find memberships in other tp
                othertps = [t.name 
                            for t in tp_obj.pure_group_members
                            if t.name in deletes and t.name != tpname]
                # Purge the object
                for oid in deletes[tpname]:
                    del self.object_map[tpname][oid]
                    deleted.setdefault(tpname, set()).add(oid)
                    for othertpname in othertps:
                        if oid in deletes[othertpname]:
                            del self.object_map[othertpname][oid]
                            deleted.setdefault(othertpname, set()).add(oid)
                    records.append((Event.Delete, tpname, oid))
                    del self.current_state[tpname][oid]
            else:
                # It is just a normal membership change. Do not ndelete the base object.
                for oid in deletes[tpname]:
                    if tpname in deleted and oid in deleted[tpname]:
                        continue
                    remaining.setdefault(tp_obj, set()).add(oid)
        # If all memberships relevant to the object are deleted, delete the actual object as well.
        for tp_obj in remaining:
            tpname = tp_obj.name
            for oid in remaining[tp_obj]:
                del self.object_map[tpname][oid]
                group_id_map.setdefault(tp_obj.group_key, dict()).setdefault(oid, set()).add(tp_obj)
                records.append((Event.Delete, tpname, oid))
        for g_obj in group_id_map:
            gname = g_obj.name
            gcount = len(g_obj.pure_group_members)
            for oid in group_id_map[gname]:
                if gcount == len(group_id_map[g_obj][oid]):
                    del self.object_map[gname][oid]
                    del self.current_state[gname][oid]
                    records.append((Event.Delete, gname, oid))

        return part_obj_map, group_changes_json, records, buffer_changes

    def __create_objs(self, all_changes, name2type):
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
        buffer_changes = list()

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
                        groupname,
                        name2type)
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
                    if not (member in name2type and name2type[member].group_key == groupname and name2type[member].observable):
                        continue
                    # If the object is New, or New for this dataframe.
                    if (status == Event.New or status == Event.Modification):
                        if member not in self.object_map or oid not in self.object_map[member]:
                            # setting a new object of the required type into object_map so that it can be pulled with get and other such functions
                            self.object_map.setdefault(member, RecursiveDictionary())[oid] = change_type(new_obj, name2type[member].type)
                            # Allow changes to the object to be tracked by the dataframe
                            self.object_map[member][oid].__start_tracking__ = True
                            # Marking this object as a new for get_new dataframe call
                            buffer_changes.append((Event.New, member, oid))
                        # If this dataframe knows this object
                        else:
                            # Markin this object as a modified object for get_mod dataframe call.
                            # Changes to the base object would have already been applied, or will be applied goin forward.
                            buffer_changes.append((Event.Modification, member, oid))
                            # Should get updated through current_state update when current_state changed.
                        # If the object is being deleted.
                    elif status == Event.Delete:
                        if member in self.object_map and oid in self.object_map[member]:
                            # Addin the object to the get_deleted buffer. It is not tracked by the dataframe any more.
                            buffer_changes.append((Event.Delete, member, oid))
                            # Maintaining a list of deletes for seein membership changes later.
                            deletes.setdefault(member, set()).add(oid)
                    else:
                        raise Exception("Object change Status %s unknown" % status)

        # Returns deleted objs for each type, 
        # an object map that can be merged with the actual object map, 
        # a list of changes that were apply for record keeping        
        return deletes, part_obj_map, group_changes_json, buffer_changes

    def __build_dimension_obj(self, dim_received, group_obj, name2type):
        groupname = group_obj.name
        dim_map = RecursiveDictionary()
        super_class = group_obj.super_class
        obj = super_class()
        for dim in dim_received:
            record = dim_received[dim]
            dim_map[dim] = record
            if not hasattr(super_class, dim):
                continue
            if record["type"] == Record.OBJECT:
                new_record = RecursiveDictionary()
                new_record["type"] = Record.DICTIONARY
                new_record["value"] = record["value"]["omap"]
                dict_value = self.__process_record(new_record, name2type)
                value = self.__create_fake_class()()
                value.__dict__ = dict_value
                value.__class__ = getattr(super_class, dim)._type
            elif (record["type"] == Record.COLLECTION 
                or record["type"] == Record.DICTIONARY):
                collect = self.__process_record(record, name2type)
                value = getattr(super_class, dim)._type(collect)
            else:    
                value = self.__process_record(record, name2type)
            setattr(obj, dim, value)
        return obj, dim_map

    def __process_record(self, record, name2type):
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
                
            dict_value = self.__process_record(new_record, name2type)
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
                    (self.__process_record(p["k"], name2type), self.__process_record(p["v"], name2type)) 
                    for p in record["value"]])
        if record["type"] == Record.FOREIGN_KEY:
            # value -> {"group_key": group key, 
            #           "actual_type": {"name": type name, "type_pickled": pickle form of type}, 
            #           "object_key": object key}
            groupname = record["value"]["group_key"]
            oid = record["value"]["object_key"]
            if groupname not in name2type or name2type[groupname].type != name2type[groupname].group_key:
                # This type cannot be created, it is not registered with the DataframeModes
                return None
            actual_type_name = (record["value"]["actual_type"]["name"]
                                 if "actual_type" in record["value"] and "name" in record["value"]["actual_type"] else
                                groupname)
            actual_type_name, actual_type = (
                            (actual_type_name, name2class[actual_type_name].type)
                              if (actual_type_name in name2class) else
                            (groupname, name2class[groupname].type))
            
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
