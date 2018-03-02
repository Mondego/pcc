
import os
import datetime
import copy
import time
import shutil
from uuid import uuid4
from dateutil import parser
from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary
from rtypes.dataframe.type_manager import TypeManager
from rtypes.pcc.types.parameter import ParameterMode
from rtypes.pcc.utils.enums import PCCCategories
from rtypes.dataframe.dataframe_changes import IDataframeChanges as df_repr
from rtypes.dataframe.dataframe_type import object_lock
from rtypes.pcc.create import create, change_type
from rtypes.pcc.utils._utils import ValueParser
from rtypes.pcc.utils.enums import Event, Record
from rtypes.dataframe.type_state import TypeState
from rtypes.dataframe.file_state_recorder import FileStateRecorder


#################################################
#### Object Management Stuff (Atomic Needed) ####
#################################################


class StateManager(object):
    save_store = ".RtypesStateManagerState"
    def __init__(self, clear_saved_state):
        # A map of the form
        # self.type_to_obj_dimstate = {
        #     group_key1 : {
        #         oid1 : RecursiveDictionary({  RecursiveDictionary is ordered.
        #             timestamp1 : {
        #                 "dims": <dimension changes>
        #             },
        #             timestamp2: {
        #                 "dims": <dimension changes>
        #             }, ... More timestamps
        #         }), ... More objects
        #     }, ... More groups
        # }
        self.type_to_obj_dimstate = dict()

        # self.type_to_obj_objids = {
        #     tpname1: set([oid1, oid2, ...]),
        #     tpname2: set([...]), ...}
        self.type_to_objids = dict()
        self.type_manager = TypeManager()
        if clear_saved_state:
            self.__clear_saved_state()
            self.__create_save_folder()


    #################################################
    ### Static Methods ##############################
    #################################################


    #################################################
    ### API Methods #################################
    #################################################

    def add_types(self, types):
        pairs = self.type_manager.add_types(
            types, check_new_type_predicate=True)
        self.create_tables(pairs)

    def add_type(self, tp):
        pairs = self.type_manager.add_type(tp)
        self.create_tables(pairs)

    def create_tables(self, tpnames_basetype_pairs):
        with object_lock:
            for tpname, _ in tpnames_basetype_pairs:
                self.__create_table(tpname)

    def apply_changes(self, df_changes):
        if "gc" in df_changes:
            self.__apply_changes(df_changes["gc"])

    def clear_all(self):
        for tpname in self.type_to_objids:
            if tpname in self.type_to_obj_dimstate:
                self.type_to_obj_dimstate[tpname].clear()
            self.type_to_objids[tpname].clear()

    def get_records(self, changelist):
        final_record = RecursiveDictionary()
        pcc_types_to_process = set()
        no_change_groups = set()
        for tpname in changelist:
            if tpname in self.type_to_obj_dimstate:
                # It is a base type. Can pull dim state for it directly.
                new_oids, mod_oids, del_oids = (
                    self.__get_oid_change_buckets(tpname, changelist[tpname]))
                tp_record = (
                    self.__get_dim_changes_for_basetype(
                        tpname, changelist[tpname],
                        new_oids, mod_oids, del_oids))
                if tp_record:
                    final_record[tpname] = tp_record
                    self.__set_type_change_status(
                        tpname, new_oids, mod_oids, del_oids,
                        final_record[tpname])
                else:
                    no_change_groups.add(tpname)
            else:
                pcc_types_to_process.add(tpname)

        for tpname in pcc_types_to_process:
            tp_obj = self.type_manager.get_requested_type_from_str(tpname)
            groupname = tp_obj.group_key
            new_oids, mod_oids, del_oids = self.__get_oid_change_buckets(
                tpname, changelist[tpname])
            metadata = tp_obj.metadata

            tp_record = dict()
            if (groupname not in final_record
                    and groupname not in no_change_groups):
                # The client is only pulling pcc record of this type. Not the
                # main type as well. We havent built updates, so pull updates.
                tp_record = (
                    self.__get_dim_changes_for_basetype(
                        groupname, changelist[tpname],
                        new_oids, mod_oids, del_oids,
                        projection_dims=metadata.projection_dims))
                if not tp_record:
                    final_record[groupname] = tp_record

            # If there are new objs, deleted objects or some objects actually
            # changed, then set status of the types.
            if new_oids or del_oids or tp_record:
                final_record[groupname] = tp_record
                self.__set_type_change_status(
                    tpname, new_oids, mod_oids if tp_record else set(),
                    del_oids, final_record[groupname])
                self.__set_latest_versions(
                    groupname, new_oids, final_record[groupname])

        return {"gc": final_record}

    #################################################
    ### Private Methods #############################
    #################################################

    def __clear_saved_state(self):
        if os.path.exists(StateManager.save_store):
            shutil.rmtree(StateManager.save_store)

    def __create_save_folder(self):
        if not os.path.exists(StateManager.save_store):
            os.makedirs(StateManager.save_store)

    def __set_latest_versions(self, tpname, new_oids, final_changes):
        for oid in new_oids:
            final_changes[oid]["version"] = [
                None, self.type_to_obj_dimstate[tpname][oid].lastkey()]

    def __set_type_change_status(
            self, tpname, new_oids, mod_oids, del_oids, changes):
        for oid in new_oids:
            changes.setdefault(oid, dict()).setdefault(
                "types", RecursiveDictionary())[tpname] = Event.New
        for oid in del_oids:
            changes.setdefault(oid, dict()).setdefault(
                "types", RecursiveDictionary())[tpname] = Event.Delete
        for oid in mod_oids:
            # If there are no dim changes, it cannot be modification.
            if oid not in changes:
                continue
            changes[oid].setdefault(
                "types", RecursiveDictionary())[tpname] = (
                    Event.Modification)

    def __get_oid_change_buckets(self, tpname, changelist):
        new_oids = self.type_to_objids[tpname].difference(
            set(changelist.keys()))
        mod_oids = self.type_to_objids[tpname].intersection(
            set(changelist.keys()))
        del_oids = set(changelist.keys()).difference(
            self.type_to_objids[tpname])
        return new_oids, mod_oids, del_oids


    def __get_dim_changes_for_basetype(
            self, tpname, changelist, new_oids,
            mod_oids, del_oids, projection_dims=None):
        group_changes = self.type_to_obj_dimstate[tpname]
        final_record = dict()
        for oid in new_oids:
            final_record[oid] = self.__merge_records(
                group_changes.get_full_obj(oid), projection_dims)
            final_record[oid]["version"] = [
                None,
                group_changes.lastkey(oid)]
        for oid in del_oids:
            final_record[oid] = dict()
        for oid in mod_oids:
            curr_vn = changelist[oid]
            dim_changes = self.__merge_records(
                group_changes.get_dim_changes_since(oid, curr_vn),
                projection_dims)
            if dim_changes:
                final_record[oid] = dim_changes
                final_record[oid]["version"] = [
                    changelist[oid],
                    group_changes.lastkey(oid)]

        return final_record

    def __merge_records(self, records, projection_dims):
        if not records:
            return dict()
        # Doing a deep copy so that we dont make modifications on existing
        # changelists by mistake.
        try:
            record1_copy = copy.deepcopy(records.next())
        except StopIteration:
            return dict()

        projection_dims_str = (
            set([d._name for d in projection_dims])
            if projection_dims else
            set())
        if projection_dims_str:
            final_record = {
                "dims": {
                    d: v
                    for d, v in record1_copy.setdefault(
                        "dims", dict()).iteritems()
                    if d in projection_dims_str}}
        else:
            final_record = record1_copy
        final_record_dims = final_record.setdefault("dims", dict())

        for rec in records:
            for dim, value in (
                    rec["dims"] if "dims" in rec else dict()).iteritems():
                if projection_dims:
                    # In case of projection, copy only req dimensions.
                    if dim in projection_dims_str:
                        final_record_dims[dim] = value
                else:
                    final_record_dims[dim] = value
        return final_record

    def __apply_changes(self, df_changes):
        '''
        all_changes is a dictionary in this format

        { <- gc stands for group changes
            "group_key1": { <- Group key for the type EG: Car
                "object_key1": { <- Primary key of object
                    "dims": { <- optional
                        "dim_name": { <- EG "velocity"
                            "type": <Record type, EG Record.INT.
                                        Enum values can be found in
                                        Record class>
                            "value": <Corresponding value,
                                        either a literal, or a collection,
                                        or a foreign key format.
                                        Can be optional if type is Null
                        },
                        <more dim records> ...
                    },
                    "types": {
                        "type_name": <status of type. Enum values can be
                                        found in Event class>,
                        <More type to status mappings>...
                    },
                    "version": [old_version, new_version]
                },
                <More object change logs> ...
            },
            <More group change logs> ...
        }
        '''
        tm = self.type_manager
        next_timestamp = time.time()
        objects_to_check_pcc = dict()
        deleted_objs = dict()
        for groupname, group_changes in df_changes.iteritems():
            if groupname not in self.type_to_obj_dimstate:
                continue
            group_changelist = self.type_to_obj_dimstate[groupname]

            for oid, obj_changes in group_changes.iteritems():
                prev_version, curr_version = obj_changes["version"]
                if not group_changelist.has_obj(oid):
                    if "dims" in obj_changes and prev_version is None:
                        # Should be a new object.
                        group_changelist.add_obj(
                            oid, curr_version, {"dims": obj_changes["dims"]})
                        objects_to_check_pcc.setdefault(
                            groupname, set()).add(oid)
                        self.type_to_objids[groupname].add(oid)
                    elif "dims" in obj_changes:
                        raise RuntimeError(
                            "Something went wrong. Obj not in record, "
                            "but has last known version? What gives?")
                    # No dims no object, ignore and continue
                    continue
                if (groupname in obj_changes["types"]
                        and obj_changes["types"] == Event.Delete):
                    # Delete all records of the object. Not required any more.
                    group_changelist.delete_obj(oid)
                    deleted_objs.setdefault(groupname, set()).add(oid)
                    continue
                # Not a delete or a new object. (modification)
                objects_to_check_pcc.setdefault(
                    groupname, set()).add(oid)
                server_last_version = group_changelist.lastkey(oid)
                # latest version number might not be curr_version in case
                # of having to do a merge update.
                if server_last_version == prev_version:
                    # Alright, no need for transformations. Straightforward
                    # merge.
                    group_changelist.add_next_change(
                        oid, curr_version, {"dims": obj_changes["dims"]})
                    # Do not need to change latest_version_number
                else:
                    # Have to do a merge.
                    changes_from_prev = self.__merge_records(
                        group_changelist.get_dim_changes_since(
                            oid, prev_version), None)
                    transformation = self.__calculate_transform(
                        changes_from_prev, {"dims": obj_changes["dims"]})
                    group_changelist.add_transformation(
                        oid, curr_version, {
                            "next_timestamp": next_timestamp,
                            "transform": transformation})
                    group_changelist.add_next_change(
                        oid, next_timestamp, {"dims": obj_changes["dims"]})

        # start pcc calculations now.
        for groupname, oids in deleted_objs.iteritems():
            try:
                group_type = tm.get_requested_type_from_str(groupname)
            except TypeError:
                continue
            for pcc_type in group_type.pure_group_members:
                if pcc_type.name in self.type_to_objids:
                    self.type_to_objids[pcc_type.name].difference_update(oids)
        
        for groupname, oids in objects_to_check_pcc.iteritems():
            try:
                group_type = tm.get_requested_type_from_str(groupname)
            except TypeError:
                continue
            for oid in oids:
                dim_changes = df_changes[groupname][oid]["dims"]
                dims_touched = set(dim_changes.keys())
                pccs_to_check = [
                    pcc_type
                    for pcc_type in group_type.pure_group_members
                    if pcc_type.metadata.dim_triggers_str.difference(
                        dims_touched) == set()]
                for pcc_type in pccs_to_check:
                    metadata = pcc_type.metadata
                    if PCCCategories.subset in metadata.categories:
                        if pcc_type.metadata.predicate(
                                *(ValueParser.parse(dim_changes[d])
                                for d in pcc_type.metadata.dim_triggers_str)):
                            self.type_to_objids[pcc_type.name].add(oid)
                        elif oid in self.type_to_objids[pcc_type.name]:
                            self.type_to_objids[pcc_type.name].remove(oid)
                    if PCCCategories.projection in metadata.categories:
                        self.type_to_objids[pcc_type.name].add(oid)

    def __calculate_transform(self, inplace_changes, new_changes):
        new_changes = {"dims": dict()}
        if "dims" in inplace_changes:
            for dimname, dimchange in inplace_changes["dims"].iteritems():
                if dimname not in inplace_changes["dims"]:
                    new_changes["dims"][dimname] = dimchange
        return new_changes

    def __create_table(self, tpname):
        tp_obj = self.type_manager.get_requested_type_from_str(tpname)
        self.type_to_obj_dimstate.setdefault(
            tp_obj.group_key, FileStateRecorder(StateManager.save_store, tp_obj.group_key))
        self.type_to_objids.setdefault(tp_obj.group_key, set())
        self.type_to_objids.setdefault(tpname, set())
