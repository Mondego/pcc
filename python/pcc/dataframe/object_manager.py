from multiprocessing import RLock, Process
from uuid import uuid4
from pcc.recursive_dictionary import RecursiveDictionary
from pcc.parameter import ParameterMode


import pcc.dataframe_changes.IDataframeChanges as df_repr
from pcc.dataframe_changes.IDataframeChanges import Event, Record

from pcc.create import create, change_type

class ChangeRecord(object):
    def __init__(self, event, tp_obj, oid, dim_change, full_obj, fk_type = None, deleted_obj = None):
        self.event = event
        self.tpname = tp_obj.name
        self.groupname = tp_obj.group_key
        self.oid = oid
        self.dim_change = dim_change
        self.full_obj = full_obj
        self.fk_type = fk_type.name if fk_type else None
        self.deleted_obj = deleted_obj

#################################################
#### Object Management Stuff (Atomic Needed) ####
#################################################

    
class ObjectManager(object):
    def __init__(self, type_manager, calculate_pcc = True):
        self. lock = RLock()
        # <group key> -> id -> object state. (Might have to make this even better)
        # object state is {"base": base state, "type 1": extra fields etc., ...}
        self.current_state = dict()

        self.object_map = dict()

        self.calculate_pcc = calculate_pcc

        self.deleted_objs = RecursiveDictionary()

        self.type_manager = type_manager

        self.changelog = RecursiveDictionary()

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
        for obj in pcc_objects:
            try:
                oid = obj.__primarykey__
            except AttributeError:
                obj.__primarykey__ = str(uuid4())

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
                    raise TypeError("Cannot construct parameter type %s without right parameters" % pcctype.__realname__)
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
            ObjectManager.__construct_pccs(tp, pccs, universe, None)
        param_collection_objs = dict([(tp_obj.type,
               universe[tp_obj.name].values()
                 if tp_obj.type not in pccs else
               pccs[tp_obj.type].values()) for tp_obj in paramtypes])

        relevant_objs = dict([(tp_obj.type,
               universe[tp_obj.name].values()
                 if tp_obj.type not in pccs else
               pccs[tp_obj.type].values()) for tp_obj in dependent_types])

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

    @staticmethod
    def __get_obj_type(obj, member_to_group):
        # both iteratable/dictionary + object type is messed up. Won't work.
        try:
            if obj.__class__.__name__ in member_to_group:
                return Record.FOREIGN_KEY
            if isinstance(obj, dict):
                return Record.DICTIONARY
            if hasattr(obj, "__iter__"):
                #print obj
                return Record.COLLECTION
            if isinstance(obj, int) or isinstance(obj, long):
                return Record.INT
            if isinstance(obj, float):
                return Record.FLOAT
            if isinstance(obj, str) or isinstance(obj, unicode):
                return Record.STRING
            if isinstance(obj, bool):
                return Record.BOOL
            if obj == None:
                return Record.NULL
            if hasattr(obj, "__dict__"):
                return Record.OBJECT
        except TypeError, e:
            return -1
        return -1

    
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
                obj_changes = ObjectManager.__convert_to_dim_map(obj_map[othertp][oid]) if event == Event.New else None
                records.extend(
                    self.__create_records(event, self.type_manager.get_requested_type(othertp), oid, obj_changes, ObjectManager.__convert_to_dim_map(obj_map[othertp][oid])))

        for othertp_obj in old_memberships:
            for oid in old_memberships[othertp_obj].difference(set(obj_map[othertp_obj.type])):
                if othertp_obj.name in self.object_map and oid in self.object_map[othertp_obj.name]:
                    records.append(ChangeRecord(Event.Delete, othertp_obj, oid, None, None))
                    del self.object_map[othertp_obj.name][oid]

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
        return records

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

            return [ChangeRecord(Event.Delete, tp_obj, oid, None, None)]
        return records

    def delete_all(self, tp_obj):
        records = []
        if tp_obj.name in self.object_map and oid in self.object_map[tp_obj.name]:
            for oid in self.object_map[tp_obj.name]:
                records.extend(self.delete(tp_obj, oid))
        return records

    def apply_changes(self, df_changes, except_df = None):
        # see __create_objs function for the format of df_changes
        # if master: send changes to all other dataframes attached.
        # apply changes to object_map, and currect_state
        # adjust pcc
        objs_new, objs_mod, objs_deleted = self.__parse_changes(df_changes)
        records, touched_objs = list(), dict()
        self.__add_new(objs_new, records, touched_objs)
        self.__change_modified(objs_mod, records, touched_objs)
        self.__delete_marked_objs(objs_deleted, records)
        pcc_adjusted_records = self.__adjust_pcc_touched(touched_objs)
        return records, records + pcc_adjusted_records
    
    def create_records_for_dim_modification(self, tp, oid, dim_change):
        records = self.__create_records(Event.Modification, self.type_manager.get_requested_type(tp.group_type), oid, dim_change, None)
        records.extend(self.__create_records(Event.Modification, 
                                     tp,
                                     oid,
                                     dim_change,
                                     None))
        return records

    def convert_to_records(self, results, deleted_oids):
        record = list()
        fks = list()
        final_record = RecursiveDictionary()
        for tp in results:
            tp_obj = self.type_manager.get_requested_type_from_str(tp)
            for obj in results[tp]:
                fk_part, obj_map = self.__convert_obj_to_change_record(obj)
                fks.extend(fk_part)
                obj_record = final_record.setdefault(tp_obj.group_key, RecursiveDictionary()).setdefault(obj.__primarykey__, RecursiveDictionary())
                obj_record.setdefault("dims", RecursiveDictionary()).rec_update(obj_map)
                obj_record.setdefault("types", RecursiveDictionary())[tp_obj.name] = Event.New

        self.__build_fk_into_objmap(fks, final_record)
        for tpname in deleted_oids:
            tp_obj = self.type_manager.get_requested_type_from_str(tpname)
            for oid in deleted_oids[tpname]:
                final_record.setdefault(
                    tp_obj.group_key, RecursiveDictionary()).setdefault(
                        oid, RecursiveDictionary()).setdefault(
                            "types", RecursiveDictionary())[tpname] = Event.Delete
        return final_record

    def convert_whole_object_map(self):
        return self.convert_to_records(
            RecursiveDictionary(
                ((tpname, objmap.values())
                for tpname, objmap in self.object_map.items())
            ), RecursiveDictionary())

    def add_buffer_changes(self, records):
        for rec in records:
            event, tpname, groupname, oid, dim_change, full_dim_map  = (
                rec.event, rec.tpname, rec.groupname, rec.oid, rec.dim_change, rec.full_obj)
            self.changelog.setdefault(event, RecursiveDictionary()).setdefault(tpname, RecursiveDictionary())[oid] = (
                self.object_map[tpname][oid]
                if event != Event.Delete else
                rec.deleted_obj)

    def get_new(self, tp):
        tpname = tp.__realname__
        return (self.changelog[Event.New][tpname].values() 
                if Event.New in self.changelog and tpname in self.changelog[Event.New] else 
                list())
    
    def get_mod(self, tp):
        tpname = tp.__realname__
        return (self.changelog[Event.Modification][tpname].values() 
                if Event.Modification in self.changelog and tpname in self.changelog[Event.Modification] else 
                list())
    
    def get_deleted(self, tp):
        tpname = tp.__realname__
        return (self.changelog[Event.Delete][tpname].values() 
                if Event.Delete in self.changelog and tpname in self.changelog[Event.Delete] else 
                list())
        
    #################################################
    ### Private Methods #############################
    #################################################

    def __convert_obj_to_change_record(self, obj):
        fks = list()
        oid = obj.__primarykey__
        dim_change_final = RecursiveDictionary()
        dim_change = self.__convert_to_dim_map(obj)
        for k, v in dim_change.items():
            dim_change_final[k._name] = self.__generate_dim(v, fks, set())
        return fks, dim_change_final

    def __adjust_pcc_touched(self, touched_objs):
        # for eadch tpname, objlist pair in the map, recalculate pccs
        records = list()
        for tp_obj, objs in touched_objs.items():
            records.extend(self.adjust_pcc(tp_obj, objs))
        return records

    def __delete_marked_objs(self, objs_deleted, records):
        # objs_deleted -> {tp_obj: [oid1, oid2, oid3, ....]}

        # first pass goes through all the base types.
        # Delete base type object, and delete pccs being calculated from that
        # For Eg: If Car is deleted, ActiveCar obj should also be deleted.
        completed_tp = set()
        for tp_obj in (tp_o 
                       for tp_o in objs_deleted 
                       if (tp_o.group_type == tp_o.type
                           and tp_o.group_key in self.object_map)):
            completed_tp.add(tp_obj)
            for oid in objs_deleted[tp_obj]:
                if oid not in self.deleted_objs.setdefault(tp_obj, set()):
                    self.deleted_objs[tp_obj].add(oid)
                    if oid in self.object_map[tp_obj.group_key]:
                        self.object_map[tp_obj][oid].start_tracking = False
                        records.append(ChangeRecord(
                            Event.Delete, 
                            self.type_manager.get_requested_type(tp_obj.group_type), 
                            oid, 
                            None, 
                            None,
                            deleted_obj = self.object_map[tp_obj.group_key][oid]))
                        del self.object_map[tp_obj.group_key][oid]

                    for pure_related_pccs_tp in tp_obj.pure_group_members:
                        if oid in self.object_map[pure_related_pccs_tp.name]:
                            self.object_map[pure_related_pccs_tp.name][oid].start_tracking = False
                            records.append(ChangeRecord(
                                Event.Delete, 
                                pure_related_pccs_tp, 
                                oid,
                                None, 
                                None,
                                deleted_obj = self.object_map[pure_related_pccs_tp.name][oid]))
                            del self.object_map[pure_related_pccs_tp.name][oid]
                            
                    del self.current_state[tp_obj.group_key][oid]

        for tp_obj in (tp for tp in objs_deleted if tp not in completed_tp):
            for oid in objs_deleted[tp_obj]:
                if oid not in self.deleted_objs.setdefault(tp_obj, set()):
                    self.deleted_objs[tp_obj].add(oid)
                    if oid in self.object_map[tp_obj.name]:
                        self.object_map[tp_obj][oid].start_tracking = False
                        records.append(ChangeRecord(
                            Event.Delete, 
                            tp_obj, 
                            oid, 
                            None, 
                            None,
                            deleted_obj = self.object_map[tp_obj.name][oid]))
                        del self.object_map[tp_obj.name][oid]
                        if len([othertp for othertp in tp_obj.group_members if othertp.name in self.object_map and oid in self.object_map[othertp.name]]) == 0:
                            del self.current_state[tp_obj.group_key][oid]
                            # delete the original object as well

    def __change_modified(self, objs_mod, records, touched_objs):
        for tp_obj in objs_mod:
            if tp_obj.name not in self.object_map:
                continue
            for oid, obj_and_change in objs_mod[tp_obj].items():
                obj, change = obj_and_change
                if oid not in self.object_map[tp_obj.name]:
                    # Treat as a new object
                    # Not sure what to do.
                    pass
                else:
                    self.object_map[tp_obj.name][oid].__dict__.rec_update(obj.__dict__)
                touched_objs.setdefault(tp_obj, RecursiveDictionary())[oid] = (self.object_map[tp_obj.name][oid])
                records.extend(
                    self.__create_records(Event.Modification, tp_obj, oid, change, None, True))

    def __add_new(self, objs_new, records, touched_objs):
        for tp_obj in objs_new:
            tp_current_state = self.current_state.setdefault(
                tp_obj.group_key, 
                RecursiveDictionary())
            for oid, obj_and_change in objs_new[tp_obj.name].items():
                obj, change = obj_and_change
                tp_current_state.setdefault(oid, RecursiveDictionary()).rec_update(obj.__dict__)
                obj.__dict__ = tp_current_state[oid]
                self.object_map.setdefault(tp_obj.name, RecursiveDictionary())[oid] = obj
                touched_objs.setdefault(tp_obj, RecursiveDictionary())[oid] = (self.object_map[tp_obj.name][oid])
                records.extend(
                    self.__create_records(Event.New, tp_obj, oid, change, None, True))


    def __parse_changes(self, df_changes):
        generated_obj_map = RecursiveDictionary()
        objs_new, objs_mod, objs_deleted = RecursiveDictionary(), RecursiveDictionary(), RecursiveDictionary()
        if "gc" not in df_changes:
            df_changes["gc"] = {}
        for groupname, group_changes in df_changes["gc"].items():
            try:
                group_type = self.type_manager.get_requested_type_from_str(groupname)
            except TypeError:
                continue
            for oid, obj_changes in group_changes.items():
                if groupname in self.deleted_objs and oid in self.deleted_objs:
                    continue
                
                final_objjson = RecursiveDictionary()
                new_obj = None
                dim_map = RecursiveDictionary()

                # If there are dimension changes to pick up
                if "dims" in obj_changes and len(obj_changes["dims"]) > 0:
                    new_obj, dim_map = self.__build_dimension_obj(
                        obj_changes["dims"], 
                        group_type)

                # For all type and status changes for that ob ject
                for member, status in obj_changes["types"].items():
                    # If member is not tracked by the dataframe
                    name2type = self.type_manager.get_name2type_map()
                    if not (member in name2type and name2type[member].group_key == groupname and name2type[member].observable):
                        continue
                    # If the object is New, or New for this dataframe.
                    if (status == Event.New or status == Event.Modification):
                        if member not in self.object_map or oid not in self.object_map[member]:
                            objs_new.setdefault(self.type_manager.get_requested_type_from_str(member), RecursiveDictionary())[oid] = new_obj, obj_changes["dims"]
                        # If this dataframe knows this object
                        else:
                            # Markin this object as a modified object for get_mod dataframe call.
                            # Changes to the base object would have already been applied, or will be applied goin forward.
                            objs_mod.setdefault(self.type_manager.get_requested_type_from_str(member), RecursiveDictionary())[oid] = new_obj, obj_changes["dims"]
                            # Should get updated through current_state update when current_state changed.
                        # If the object is being deleted.
                    elif status == Event.Delete:
                        if member in self.object_map and oid in self.object_map[member]:
                            # Maintaining a list of deletes for seein membership changes later.
                            objs_deleted.setdefault(self.type_manager.get_requested_type_from_str(member), set()).add(oid)
                    else:
                        raise Exception("Object change Status %s unknown" % status)
        return objs_new, objs_mod, objs_deleted
            
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
        records.extend(
            self.__create_records(Event.New, tp_obj, oid, obj_changes, obj_changes))
        return records
        
    def __get(self, tp_obj, parameter):
        tp = tp_obj.type
        tpname = tp_obj.name
        with self.lock:
            if tp_obj.is_pure:
                return self.object_map[tpname] if tpname in self.object_map else dict()
            obj_map = ObjectManager.build_pccs([tp_obj], self.object_map, parameter)
            return obj_map[tp] if tp in obj_map else dict()

    def __apply(self, df_changes):
        records = list()
        objs_to_be_deleted, obj_map, buffer_changes = self.__create_objs(df_changes)
        records.extend(self.__apply_delete(objs_to_be_deleted))
        for tp_obj, objs in obj_map.items():
            records.extend(self.adjust_pcc(tp_obj, objs))
        return records, buffer_changes

    def __create_objs(self, df_changes):
        for groupname, groupchanges in df_changes["gc"].items():
            group_type = self.type_manager.get_requested_type_from_str(groupname)
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
                        group_type,
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
                    obj_change_json.setdefault("dims", RecursiveDictionary()).rec_update(dim_map)
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

    def __apply_delete(self, objs_to_be_deleted):
        pass

    def __apply(self, df_changes):
        # See __create_objs to see the format of df_changes
        part_obj_map = dict()
        group_changes_json = RecursiveDictionary()
        #Objects are being created and applied to the object_map
        deletes, part_obj_map, group_changes_json, buffer_changes = self.__create_objs(df_changes)
        
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
                    records.extend((Event.Delete, tp_obj, oid, None, None))
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
                records.append((Event.Delete, tp_obj, oid, None, None))
        for g_obj in group_id_map:
            gname = g_obj.name
            gcount = len(g_obj.pure_group_members)
            for oid in group_id_map[gname]:
                if gcount == len(group_id_map[g_obj][oid]):
                    del self.object_map[gname][oid]
                    del self.current_state[gname][oid]
                    records.append((Event.Delete, g_obj, oid, None, None))

        return part_obj_map, group_changes_json, records, buffer_changes

    def __create_records(self, event, tp_obj, oid, obj_changes, full_obj_map, converted = False, fk_type_to = None):
        records = list()
        fks = list()
        new_obj_changes = RecursiveDictionary()
        new_full_obj_map = RecursiveDictionary()
        if converted:
            records.append(ChangeRecord(event, tp_obj, oid, obj_changes, full_obj_map, fk_type_to))
            if obj_changes:
                for k, v in obj_changes.items():
                    if v["type"] == Record.FOREIGN_KEY:
                        fk = v["value"]["object_key"]
                        fk_event = Event.Modification if v["value"]["group_key"] in self.object_map and fk in self.object_map[v["value"]["group_key"]] else Event.New
                        fk_type_obj = self.type_manager.get_requested_type_from_str(v["value"]["actual_type"]["name"])
                        fk_full_obj = self.__convert_to_dim_map(self.object_map[group][fk])
                        if fk_event == Event.New and group in self.object_map and fk in self.object_map[group]:
                            fk_dims = fk_full_obj          
                        records.extend(self.__create_records(fk_event, fk_type_obj, fk, fk_dims, fk_full_obj, fk_type_to = tp_obj))
            if full_obj_map:
                for k, v in full_obj_map.items():
                    if v["type"] == Record.FOREIGN_KEY:
                        fk = v["value"]["object_key"]
                        fk_event = Event.Modification if v["value"]["group_key"] in self.object_map and fk in self.object_map[v["value"]["group_key"]] else Event.New
                        fk_type_obj = self.type_manager.get_requested_type_from_str(v["value"]["actual_type"]["name"])
                        fk_full_obj = self.__convert_to_dim_map(self.object_map[group][fk])
                        if fk_event == Event.New and group in self.object_map and fk in self.object_map[group]:
                            fk_dims = fk_full_obj          
                        records.extend(self.__create_records(fk_event, fk_type_obj, fk, fk_dims, fk_full_obj, fk_type_to = tp_obj))
            return records

        if obj_changes:
            for k, v in obj_changes.items():
                new_obj_changes[k._name] = self.__generate_dim(v, fks, set())
        if full_obj_map:
            if full_obj_map == obj_changes:
                new_full_obj_map = new_obj_changes
            else:
                for k, v in full_obj_map.items():
                    new_full_obj_map[k._name] = self.__generate_dim(v, fks, set())
        for fk, fk_type_obj in fks: 
            group = fk_type_obj.group_key
            fk_event_type = Event.Modification if group in self.object_map and fk in self.object_map[group] else Event.New
            fk_dims = None
            fk_full_obj = self.__convert_to_dim_map(self.object_map[group][fk])
            if fk_event_type == Event.New and group in self.object_map and fk in self.object_map[group]:
                fk_dims = fk_full_obj
            records.extend(self.__create_records(fk_event_type, fk_type_obj, fk, fk_dims, fk_full_obj, fk_type_to = tp_obj))
        records.append(ChangeRecord(event, tp_obj, oid, new_obj_changes, new_full_obj_map, fk_type_to))
        return records

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
        buffer_changes = list()
        name2type = self.type_manager.get_name2type_map()

        # For each group
        for groupname, groupchanges in all_changes["gc"].items():
            group_type = self.type_manager.get_requested_type_from_str(groupname)
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
                        group_type)
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
                    obj_change_json.setdefault("dims", RecursiveDictionary()).rec_update(dim_map)
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

    def __build_dimension_obj(self, dim_received, group_obj):
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
                dict_value = self.__process_record(new_record)
                value = self.__create_fake_class()()
                value.__dict__ = dict_value
                value.__class__ = getattr(super_class, dim)._type
            elif (record["type"] == Record.COLLECTION 
                or record["type"] == Record.DICTIONARY):
                collect = self.__process_record(record)
                value = getattr(super_class, dim)._type(collect)
            else:    
                value = self.__process_record(record)
            setattr(obj, dim, value)
        return obj, dim_map

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
            name2type = self.type_manager.get_name2type_map()
            if groupname not in name2type or name2type[groupname].type != name2type[groupname].group_key:
                # This type cannot be created, it is not registered with the DataframeModes
                return None
            actual_type_name = (record["value"]["actual_type"]["name"]
                                 if "actual_type" in record["value"] and "name" in record["value"]["actual_type"] else
                                groupname)
            actual_type_name, actual_type = (
                            (actual_type_name, self.type_manager.get_name2type_map()[actual_type_name].type)
                              if (actual_type_name in self.type_manager.get_name2type_map()) else
                            (groupname, self.type_manager.get_name2type_map()[groupname].type))
            
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

    def __generate_dim(self, dim_change, foreign_keys, built_objs):
        try:
            if dim_change in built_objs:
                raise RuntimeError("Cyclic reference in the object to be serialized. %s", dim_change)
        except TypeError:
            pass
        dim_type = ObjectManager.__get_obj_type(dim_change, self.type_manager.member_to_group)
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
            dim["value"] = [self.__generate_dim(v, foreign_keys, built_objs) for v in dim_change]
            return dim                
        
        if dim_type == Record.DICTIONARY:
            dim["value"] = [RecursiveDictionary({"k": self.__generate_dim(k, foreign_keys, built_objs), 
                                                 "v": self.__generate_dim(v, foreign_keys, built_objs)}) 
                            for k, v in dim_change.items()]
            return dim
            
        if dim_type == Record.OBJECT:
            try:
                built_objs.add(dim_change)
            except TypeError:
                pass
            dim["value"] = RecursiveDictionary()
            dim["value"]["omap"] = (
                self.__generate_dim(dim_change.__dict__, foreign_keys, built_objs)["value"])
            # Can also set the type of the object here serialized. Future work. 
            return dim

        if dim_type == Record.FOREIGN_KEY:
            key, fk_type_obj = dim_change.__primarykey__, self.type_manager.get_requested_type(dim_change.__class__)
            if fk_type_obj.saveable_parent:
                convert_type = fk_type_obj.saveable_parent
            else:
                raise TypeError("Cannot use %s as a foreign key type" % fk_type)
                        
            foreign_keys.append((key, convert_type))
            dim["value"] = RecursiveDictionary()
            dim["value"]["group_key"] = convert_type.group_key
            dim["value"]["object_key"] = key
            dim["value"]["actual_type"] = RecursiveDictionary()
            dim["value"]["actual_type"]["name"] = convert_type.name
            return dim

        raise TypeError("Don't know how to deal with %s" % dim_change)

    def __create_fake_class(self):
        class container(object):
            pass
        return container

    def __build_fk_into_objmap(self, fks, final_record):
        if len(fks) == 0:
            return

        fk, fk_type_obj = fks.pop()
        group = fk_type_obj.group_key
        fk_event_type = Event.New
        new_fks, fk_full_obj = self.__convert_obj_to_change_record(self.object_map[group][fk])
        fks.extend(new_fks)
        fk_obj_record = final_record.setdefault(fk_type_obj.group_key, RecursiveDictionary()).setdefault(fk, RecursiveDictionary())
        fk_obj_record.setdefault("dims", RecursiveDictionary()).rec_update(fk_full_obj)
        fk_obj_record.setdefault("types", RecursiveDictionary())[fk_type_obj.name] = Event.New
        self.__build_fk_into_objmap(fks, final_record)
