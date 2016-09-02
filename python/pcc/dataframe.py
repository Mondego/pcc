'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import spacetime_property
from recursive_dictionary import RecursiveDictionary
from create import create, change_type
from uuid import uuid4
from parameter import ParameterMode
from cache import Cache

class DimensionType(object):
    Literal = "literal"
    Object = "object"
    Collection = "list"
    Dictionary = "dict"

class ObjectChanges(object):
    New = "new"
    Mod = "mod"
    Delete = "delete"

class ObjectType(object):
    UnknownType = "unknown type"
    PCCBase = "pcc base"
    ISA = "isa"
    Subset = "subset"
    Join = "join"
    Projection = "projection"
    Union = "union"
    Param = "param"
    Impure = "impure"

class Event(object):
    Delete = "delete"
    New = "new"
    Modification = "modification"

class dataframe(object):
    def __init__(self, lock=None):
        # group key (always string fullanme of type) to members of group.  For example: car -> activecar,
        # inactivecar, redcar, etc etc.
        self.group_to_members = {}
        
        # str name to class.
        self.name2class = {}

        # member to groups it belongs to. (should be just one i think)
        # redcar -> car
        self.member_to_group = {}
        
        # <group key> -> id -> object state. (Might have to make this even better)
        # object state is {"base": base state, "type 1": extra fields etc., ...}
        self.current_state = {}

        self.object_map = {}

        # set of types that are impure by default and hence cannot be maintained continuously.
        self.impure = set()

        # dependent type. activecar -depends on-> car
        # pedestrianAndCarNearby -depends on-> activecar, walker, car, pedestrian (goes all the way)
        self.depends_map = {}

        self.categories = {}

        self.fake_class_map = {}

    def __get_depends(self, tp):
        if hasattr(tp, "__pcc_bases__"):
            return tp.__pcc_bases__
        raise TypeError("Type %s needs to have some bases" % tp.__realname__)

    def __get_group_key(self, tp):
        if tp.__PCC_BASE_TYPE__:
            return tp.__realname__, tp
        else:
            return self.__get_group_key(list(tp.__pcc_bases__)[0])

    def __check_validity(self, tp, obj):
        if not hasattr(tp, "__realname__"): 
            # Fail to add new obj, because tp was incompatible, or not found.
            raise TypeError("Type %s cannot be inserted into Dataframe, declare it as pcc_set." % tp.__class__.__name__)
        if not tp.__realname__ in self.member_to_group:
            raise TypeError("Type %s cannot be inserted into Dataframe, register it first." % tp.__realname__) 
        if ObjectType.PCCBase not in self.categories[tp.__realname__]:
            # Person not appending the right type of object
            raise TypeError("Cannot append type %s" % tp.__realname__)
        if type(obj) is not tp:
            raise TypeError("Object type and type given do not match")

    
    def __append(self, tp, obj):
        self.__check_validity(tp, obj)

        # all clear to insert.

        if obj.__primarykey__ == None:
            setattr(obj, tp.__primarykey__._name, uuid4())

        id = obj.__primarykey__
        tpname = tp.__realname__
        groupname = self.member_to_group[tpname]

        # Store the state in records
        self.current_state[groupname][id] = RecursiveDictionary(obj.__dict__)

        # Set the object state by reference to the original object's symbol table
        obj.__dict__ = self.current_state[groupname][id]
        self.object_map.setdefault(tpname, RecursiveDictionary())[id] = obj
        return groupname, id
    
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
        return (len(set([ObjectType.Join,
                         ObjectType.Impure,
                         ObjectType.Param,
                         ObjectType.UnknownType]).intersection(
                             categories)) > 0)

    def __adjust_pcc(self, objs, groupname):
        can_be_created = []
        for othertp in self.group_to_members[groupname]:
            other_cats = self.categories[othertp.__realname__]
            if ((not self.__is_impure(othertp, other_cats))
                and len(set([ObjectType.Projection,
                             ObjectType.Subset,
                             ObjectType.Union]).intersection(
                                 other_cats)) > 0):
                can_be_created.append(othertp)
        old_memberships = {}
        for id in objs:
            for othertp in can_be_created:
                old_memberships[othertp] = set()
                othertpname = othertp.__realname__
                if (othertpname in self.object_map 
                    and id in self.object_map[othertpname]):
                    old_memberships[othertp].add(id)

        new_objs_map = self.__calculate_pcc(can_be_created, {groupname: objs}, None)
        id_map = {}
        for othertp, new_objs in new_objs_map.items():
            for new_obj in new_objs:
                id_map.setdefault(othertp, set()).add(new_obj.__primarykey__)
                if new_obj.__primarykey__ not in old_memberships[othertp]:
                    self.record(Event.New, othertp.__realname__, new_obj.__primarykey__) 
                else:
                    self.record(Event.Modification, othertp.__realname__, new_obj.__primarykey__)
                self.object_map.setdefault(
                    othertp.__realname__, 
                    RecursiveDictionary())[new_obj.__primarykey__] = new_obj

        for othertp in old_memberships:
            for id in old_memberships[othertp].difference(set(id_map[othertp])):
                self.record(Event.Delete, othertp.__realname__, id)
                del self.object_map[othertp.__realname__][id]

    def __make_pcc(self, pcctype, relevant_objs, param_map, params, hasSingleton = False, hasCollection = False):
        universe = []
        param_list = []
        for tp in pcctype.__ENTANGLED_TYPES__:
            universe.append(relevant_objs[tp])

        # Creating random rule for parameters, all Collections first, then singleton objects
        # Why? Random, couldnt think of a better way

        if hasCollection:
            for tp in pcctype.__parameter_types__:
                param_list.append(param_map[tp])
        if hasSingleton:
            param_list.extend(params)

        try:
            pcc_objects = create(pcctype, *universe, params = param_list)
        except TypeError, e:
            print ("Exception in __make_pcc: " + e.message)
            return []
        return pcc_objects

    def __construct_pccs(self, pcctype, pccs, universe, param):
        if pcctype in pccs:
            return pccs[pcctype]
        params, paramtypes = [], []
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
        dependent_pccs = [tp for tp in (dependent_types + paramtypes) if not tp.__PCC_BASE_TYPE__]
        to_be_resolved = [tp for tp in dependent_pccs if tp not in pccs]
        for tp in to_be_resolved:
            self.__construct_pccs(tp, pccs, universe, None)
        param_collection_objs = dict([(tp,
               universe[tp.__realname__].values()
                 if tp.__PCC_BASE_TYPE__ else
               pccs[tp]) for tp in paramtypes])

        relevant_objs = dict([(tp,
               universe[tp.__realname__].values()
                 if tp.__PCC_BASE_TYPE__ else
               pccs[tp]) for tp in dependent_types])

        pccs[pcctype] = self.__make_pcc(pcctype, relevant_objs, param_collection_objs, params, hasSingleton, hasCollection)

    def __calculate_pcc(self, pcctypes, universe, params):
        pccs = {}
        for pcctype in pcctypes:
            self.__construct_pccs(pcctype, pccs, universe, params)
        return pccs

    def __process_record(self, record, nsp):
        if record["type"] == DimensionType.Literal:
            return record["value"], False or nsp
        if record["type"] == DimensionType.Object:
            dict_value, nsp_new == self.__process_record({"type": DimensionType.Dictionary, "value": record["value"]}, nsp)
            value = self.__create_fake_class()()
            # cannot set type of object (at least yet)
            value.__dict__ = dict_value
            return value, nsp_new or nsp
        if record["type"] == DimensionType.Collection:
            # Assume it is list, as again, don't know this type
            value = []
            nsp_new = False
            for rec in record["value"]:
                v, nsp_new_part = self.__process_record(rec, nsp)
                nsp_new = nsp_new or nsp_new_part
                value.append(v)
            return value, nsp or nsp_new
        if record["type"] == DimensionType.Dictionary:
            # Assume it is dictionary, as again, don't know this type
            value = {}
            nsp_new = False
            for k, v in record["value"].items():
                # Unfortunately for now k has to be string (json problem). 
                # Sigh too many rules.
                value[k], nsp_new_part = self.__process_record(v, nsp)
                nsp_new = nsp_new or nsp_new_part
            return value, nsp_new
        raise TypeError("Do not know dimension type %s", record["type"])

    def __build_dimension_obj(self, dim_received, groupname):
        fakeclass = self.fake_class_map[groupname]
        obj = fakeclass()
        needs_second_pass = False
        for dim, record in dim_received.items():
            if not hasattr(fakeclass, dim):
                continue
            if record["type"] == DimensionType.Object:
                dict_value, nsp = self.__process_record({"type": DimensionType.Dictionary, "value": record["value"]}, False)
                value = self.__create_fake_class()()
                value.__dict__ = dict_value
                value.__class__ = getattr(fakeclass, dim)._type
            elif (record["type"] == DimensionType.Collection 
                or record["type"] == DimensionType.Dictionary):
                collect, nsp =  self.__process_record(record, False)
                value = getattr(fakeclass, dim)._type(collect)
            else:    
                value, nsp = self.__process_record(record, False)
            needs_second_pass = needs_second_pass or nsp
            setattr(obj, dim, value)
        return obj, needs_second_pass

    def __create_obj_from_map(self, final_objjson):
        pass


    def add_type(self, tp, except_type = None):
        categories = self.__categorize(tp)
        # str name of the type.
        name = tp.__realname__
        
        if ObjectType.UnknownType in categories:
            raise TypeError("Type %s cannot be added" % name)
        
        # add type to map
        self.name2class[name] = tp

        # getting all the dependencies that tp depends on (results are in string form)
        depend_types = self.__get_depends(tp)
        self.depends_map[name] = depend_types

        for dtp in depend_types:
            dtpname = dtp.__realname__
            if dtpname == except_type:
                raise TypeError("Cyclic reference detected in definition of %s" % name)
            if dtpname not in self.name2class:
                self.add_type(dtp, except_type = name)
            if ObjectType.Impure in self.categories[dtpname]:
                categories.add(ObjectType.Impure)

        # add categories to map
        self.categories[name] = categories
        
        if ObjectType.Join in categories or ObjectType.Impure in categories:
            self.impure.add(tp)
        
        # Type isnt impure. things are getting interesting. I can cache it!
        # str key for the group it belongs to.
        key, keytp = self.__get_group_key(tp)
        self.name2class[key] = keytp
    
        # Adding name to the group
        self.group_to_members.setdefault(key, set()).add(tp)
        self.member_to_group[name] = key
        self.current_state.setdefault(key, RecursiveDictionary())
        self.fake_class_map.setdefault(key, self.__create_fake_class()).add_dims(tp.__dimensions__)
        

        for dim in tp.__dimensions__:
            dim._groupname = key
            dim._touch_reclassifier = self.__adjust_pcc
        
    def add_types(self, types):
        for tp in types:
            self.add_type(tp)

    def remove_type(self, tp):
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
            if tp in self.impure:
                self.impure.remove(tp)
        else:
            raise TypeError("Type %s is not in the dataframe." % tpname)

    def append(self, tp, obj):
        groupname, id = self.__append(tp, obj)
        self.__adjust_pcc({id, obj}, groupname)
        
    def extend(self, tp, objs):
        objmap = {}
        for obj in objs:
            groupname, id = self.__append(tp, obj)
            objmap[id] = obj
        self.__adjust_pcc(objmap, groupname)
    
    def get(self, tp, parameters=None):

        tpname = tp.__realname__
        if tpname not in self.name2class:
            raise TypeError("Type %s not registered" % tpname)
        
        if not self.__is_impure(tp, self.categories[tp.__realname__]):
            return self.object_map[tpname].values()

        groupkey = self.member_to_group[tpname]
        return self.__calculate_pcc([tp], self.object_map, parameters)[tp]

    def delete(self, tp, obj):
        self.__check_validity(tp)
        # all clear to delete
        id = obj.__primarykey__
        tpname = tp.__realname__
        groupname = self.member_to_group[tpname]
        del self.current_state[groupname][id]
        self.record(Event.Delete, tpname, id)
        # delete from object_map too
        for member in self.group_to_members[groupname]:
            if member.__realname__ in self.object_map and id in self.object_map[member.__realname__]:
                del self.object_map[member.__realname__][id]
        

    def remove_types(self, types):
        for tp in types:
            self.remove_type(tp)

    def reload_types(self, types):
        # maybe clear maps and data? before reloading. Will see later.
        self.add_types(types)
    
    def __pass1_objs(self, all_changes):
        current_obj_map = RecursiveDictionary()
        part_obj_map = RecursiveDictionary()

        needs_second_pass = False
        nsp_items = RecursiveDictionary()
        
        deletes = {}
        for groupname, groupchanges in all_changes.items():
            if groupname not in self.group_to_members:
                continue

            for id, obj_changes in groupchanges.items():
                final_objjson = RecursiveDictionary()
                new_obj = None
                nsp_items = {}
                if "dims" in obj_changes:
                    new_obj, nsp = self.__build_dimension_obj(obj_changes["dims"], groupname)
                    needs_second_pass = needs_second_pass or nsp
                    if nsp:
                        nsp_items.setdefault(groupname, set()).add(id)

                    if id in self.current_state[groupname]:
                        # getting actual reference if it is there.
                        final_objjson = self.current_state[groupname][id]
                    final_objjson.rec_update(new_obj.__dict__)
                    self.current_state[groupname][id] = final_objjson
                    new_obj.__dict__ = final_objjson
                for member, status in obj_changes["types"].items():
                    if not (member in self.member_to_group and self.member_to_group[member] == groupname):
                        continue
                    if status == ObjectChanges.New:
                        self.object_map.setdefault(member, RecursiveDictionary())[id] = change_type(new_obj, self.name2class[member])
                    elif status == ObjectChanges.Mod:
                        # Should get updated through current_state update when current_state changed.
                        pass
                    elif status == ObjectChanges.Delete:
                        deletes.setdefault(member, set()).add(id)
                    else:
                        raise Exception("Object change Status %s unknown" % status)


        
        return needs_second_pass, nsp_items, deletes

    def apply_all(self, obj_changes, record_change = False):
        # {tp -> {id -> {__dict__ update}}}
        needs_second_pass, nsp_items, deletes = self.__pass1_objs(obj_changes)
        if needs_second_pass:
            part_obj_map = self.__pass2_obj(obj_changes, part_obj_map)

        #self.record(part_obj_map, record_change)
        group_id_map = {}
        remaining = {}
        deleted = {}
        for tpname in deletes:
            if tpname in self.group_to_members:
                othertps = [t.__realname__ 
                            for t in self.group_to_members[tpname] 
                            if t.__realname__ in deletes and t.__realname__ is not tpname]
                for id in deletes[tpname]:
                    del self.object_map[tpname][id]
                    deleted.setdefault(tpname, set()).add(id)
                    for othertpname in othertps:
                        if id in deletes[othertpname]:
                            del self.object_map[othertpname][id]
                            deleted.setdefault(othertpname, set()).add(id)
                    del self.current_state[tpname][id]
            else:
                for id in deletes[tpname]:
                    if tpname in deleted and id in deleted[tpname]:
                        continue
                    remaining.setdefault(tpname, set()).add(id)
        for tpname in remaining:
            for id in remaining[tpname]:
                del self.object_map[tpname][id]
                group_id_map.setdefault(self.member_to_group[tpname], {}).setdefault(id, set()).add(tpname)
        for gname in group_id_map:
            gcount = len(self.group_to_members[gname])
            for id in group_id_map[gname]:
                if gcount == len(group_id_map[gname][id]):
                    del self.object_map[gname][id]
                    del self.current_state[gname][id]
    
    def record(self, event_type, tpname, id):
        pass

    def json_to_obj(self, objjson):
        pass

    def get_new(self, tp):
        pass

    def get_mod(self, tp):
        pass

    def get_deleted(self, tp):
        pass
