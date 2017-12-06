#################################################
### Type Management Stuff (Atomic via pause) ####
#################################################
import os
from rtypes.dataframe.dataframe_type import type_lock, DataframeType
from rtypes.pcc.utils.enums import PCCCategories

class TypeManager(object):

    def __init__(self):

        # group key (always string fullanme of type) to members of group.
        # For example: car -> activecar, inactivecar, redcar, etc etc.
        self.group_to_members = dict()

        # group key (always string fullanme of type)
        # to members of group that are pure.  For example: car -> activecar,
        # inactivecar, redcar, etc etc. but not CarAndPedestrian (a join)
        self.group_to_pure_members = dict()

        # str name to class.
        self.name2class = dict()

        # member to groups it belongs to. (should be just one i think)
        # redcar -> car
        self.member_to_group = dict()

        # set of types that are impure by default
        # and hence cannot be maintained continuously.
        self.impure = set()

        # dependent type. activecar -depends on-> car
        # pedestrianAndCarNearby - depends on-> activecar, walker,
        # car, pedestrian (goes all the way)
        self.depends_map = dict()

        # Categories that a type belongs to
        self.categories = dict()

        # Part class structures for dataframes that do not use sets,
        # but only projections of sets.
        self.super_class_map = dict()

        # Types that are only to be read, not written into.
        self.observing_types = set()

        # Types that are marked with @join.
        self.join_types = set()

        # The closest premamnent storable type for a registered type.
        self.closest_foreign_key_type = dict()

        self.tp_to_dataframe_payload = dict()

    #################################################
    ### Static Methods ##############################
    #################################################

    @staticmethod
    def __create_superset_class():
        class _container(object):
            @staticmethod
            def add_dims(dims):
                for dim in dims:
                    setattr(_container, dim._name, dim)
        return _container

    @staticmethod
    def __categorize(tp):
        return tp.__rtypes_metadata__.categories

    @staticmethod
    def __get_depends(tp):
        return tp.__rtypes_metadata__.parent_types

    @staticmethod
    def __get_group_key(tp):
        metadata = tp.__rtypes_metadata__
        return metadata.group_type.name, metadata.group_type.cls

    @staticmethod
    def __is_not_saveable(categories):
        return (PCCCategories.join in categories
                or PCCCategories.parameter in categories
                or PCCCategories.subset in categories
                or PCCCategories.union in categories)

    @staticmethod
    def __is_impure(tp, categories):
        return (len(set([PCCCategories.join,
                         PCCCategories.impure,
                         PCCCategories.parameter,
                         PCCCategories.unknown_type]).intersection(
                             categories)) > 0)

    #################################################
    ### API Methods #################################
    #################################################

    def add_type(
            self, tp, tracking=False, pcc_adjuster=None,
            dim_modification_reporter=None, records_creator=None):
        pairs_added = set()
        with type_lock:
            self.__add_type(
                tp, tracking=tracking, pairs_added=pairs_added,
                pcc_adjuster=pcc_adjuster,
                dim_modification_reporter=dim_modification_reporter,
                records_creator=records_creator)
        return pairs_added

    def add_types(
            self, types, tracking=False, pcc_adjuster=None,
            dim_modification_reporter=None, records_creator=None):
        pairs_added = set()
        with type_lock:
            for tp in types:
                self.__add_type(
                    tp, tracking=tracking, pairs_added=pairs_added,
                    pcc_adjuster=pcc_adjuster,
                    dim_modification_reporter=dim_modification_reporter,
                    records_creator=records_creator)
        return pairs_added

    def has_type(self, tp):
        return tp.__rtypes_metadata__.name in self.member_to_group

    def reload_types(self, types):
        pass

    def remove_type(self, tp):
        pass

    def remove_types(self, types):
        pass

    def check_for_new_insert(self, tp):
        if not hasattr(tp, "__rtypes_metadata__"):
            # Fail to add new obj, because tp was incompatible, or not found.
            raise TypeError(
                "Type %s cannot be inserted/deleted into Dataframe, "
                "declare it as pcc_set." % tp.__class__.__name__)
        metadata = tp.__rtypes_metadata__
        if metadata.name not in self.name2class:
            raise TypeError("Type %s hasnt been registered" % tp.__realname__)
        tp_obj = self.name2class[metadata.name]
        if not tp_obj in self.observing_types:
            raise TypeError(
                "Type %s cannot be inserted/deleted into Dataframe, "
                "register it first." % metadata.name)
        if (PCCCategories.pcc_set not in tp_obj.categories
               and PCCCategories.projection not in tp_obj.categories):
            # Person not appending the right type of object
            raise TypeError("Cannot insert/delete type %s" % metadata.name)
        if not hasattr(tp, "__primarykey__"):
            raise TypeError(
                "Type must have a primary key dimension "
                "to be used with Dataframes")
        return True

    def check_obj_type_for_insert(self, tp, obj):
        metadata = tp.__rtypes_metadata__
        tp_obj = self.name2class[metadata.name]
        if (metadata.name != obj.__class__.__rtypes_metadata__.name
               and tp_obj.group_key != metadata.name):
            raise TypeError("Object type and type given do not match")
        return True

    def get_requested_type(self, tp):
        return self.get_requested_type_from_str(tp.__rtypes_metadata__.name)

    def get_requested_type_from_str(self, tpname):
        try:
            if tpname in self.name2class:
                return self.name2class[tpname]
            else:
                raise TypeError("Type %s is not registered" % tpname)
        except KeyError:
            raise TypeError("Type %s is not registered" % tpname)

    def get_name2type_map(self):
        return self.name2class

    def get_impures_in_types(self, types, all=False):
        if all:
            # Passed by reference. very important!!!
            return self.impure
        result = set()
        for tp in types:
            tp_obj = self.get_requested_type(tp)
            if (PCCCategories.impure in tp_obj.categories
                or PCCCategories.join in tp_obj.categories
                or PCCCategories.parameter in tp_obj.categories
                or PCCCategories.union in tp_obj.categories):
                result.add(tp)
        return result

    def get_join_types(self):
        return self.join_types


    #################################################
    ### Private Methods #############################
    #################################################

    def __check_type(self, tp):
        if not hasattr(tp, "__rtypes_metadata__"):
            raise TypeError("Type {0} has to be a PCC Type".format(repr(tp)))

    def __add_type(
            self, tp, except_type=None, tracking=False, not_member=False,
            pairs_added=set(), pcc_adjuster=None,
            dim_modification_reporter=None, records_creator=None):
        self.__check_type(tp)
        metadata = tp.__rtypes_metadata__
        name = metadata.name
        categories = TypeManager.__categorize(tp)
        if PCCCategories.unknown_type in categories:
            raise TypeError("Type %s cannot be added" % name)
        if metadata.group_dimensions:
            categories.add(PCCCategories.impure)
        key, keytp = TypeManager.__get_group_key(tp)
        tp_obj = DataframeType(tp, keytp, categories)
        if key in self.name2class:
            key_obj = self.name2class[key]
        elif key != name:
            key_obj = self.__add_type(
                keytp, except_type=tp, not_member=True,
                pcc_adjuster=pcc_adjuster,
                dim_modification_reporter=dim_modification_reporter,
                records_creator=records_creator)
            self.name2class[key] = key_obj
        else:
            key_obj = tp_obj
        self.name2class[name] = tp_obj

        if not not_member:
            self.observing_types.add(tp_obj)
        #tp_obj.observable = not not_member

        not_directly_saveable_type = TypeManager.__is_not_saveable(categories)
        if not not_directly_saveable_type:
            tp_obj.saveable_parent = tp_obj
        cannot_be_saved = (
            PCCCategories.join in categories
            or PCCCategories.union in categories)
        # getting all the dependencies that tp depends on.
        # (results are in string form)
        depend_types = TypeManager.__get_depends(tp)
        self.depends_map[name] = depend_types

        depends = []
        for dtp in depend_types:
            dtp_metadata = dtp.__rtypes_metadata__
            dtpname = dtp_metadata.name
            if dtpname == except_type:
                raise TypeError(
                    "Cyclic reference detected in definition of %s" % name)
            if dtpname not in self.name2class:
                dtp_obj =  self.__add_type(
                    dtp, except_type=name, not_member=True,
                    pcc_adjuster=pcc_adjuster,
                    dim_modification_reporter=dim_modification_reporter,
                    records_creator=records_creator)
            else:
                dtp_obj = self.name2class[dtpname]
            if PCCCategories.impure in dtp_obj.categories:
                categories.add(PCCCategories.impure)
            if (not cannot_be_saved
                and not_directly_saveable_type
                and dtp_obj.can_be_persistent):
                tp_obj.saveable_parent = dtp_obj.saveable_parent
            depends.append(dtp_obj)

        tp_obj.is_pure = not TypeManager.__is_impure(tp, categories)
        tp_obj.depends = depends
        if metadata.parameter_types:
            for mode, types in metadata.parameter_types.iteritems():
                ptype_objs = []
                for ptp in types:
                    insert_obj = ptp
                    if hasattr(ptp, "__rtypes_metadata__"):
                        ptp_metadata = ptp.__rtypes_metadata__
                        insert_obj = (
                            self.name2class[ptp_metadata.name]
                            if ptp_metadata.name in self.name2class else
                            self.__add_type(
                                ptp, except_type=name,
                                not_member=True, pcc_adjuster=pcc_adjuster,
                                dim_modification_reporter=\
                                    dim_modification_reporter,
                                records_creator=records_creator))
                    ptype_objs.append(insert_obj)
                if tp_obj.parameter_types == dict():
                    tp_obj.parameter_types = dict()
                tp_obj.parameter_types[mode] = ptype_objs

        # Adding name to the group
        self.group_to_members.setdefault(key, set()).add(tp_obj)
        self.group_to_pure_members.setdefault(key, set())
        if (tp != keytp
                and (not TypeManager.__is_impure(tp, categories))
                and len(set([
                    PCCCategories.projection,
                    PCCCategories.subset,
                    PCCCategories.union]).intersection(
                        categories)) > 0):
            self.group_to_pure_members[key].add(tp_obj)

        tp_obj.group_members = self.group_to_members[key]
        tp_obj.pure_group_members = self.group_to_pure_members[key]
        pairs_added.add((name, PCCCategories.pcc_set in categories))
        self.super_class_map.setdefault(
            key, self.__create_superset_class()).add_dims(metadata.dimensions)
        tp_obj.super_class = self.super_class_map[key]

        if PCCCategories.join in categories:
            self.join_types.add(tp_obj)

        self.tp_to_dataframe_payload[tp_obj] = (
            (self.get_requested_type, pcc_adjuster, records_creator,
             dim_modification_reporter)
            if not metadata.group_dimensions else
            None)
        return tp_obj
