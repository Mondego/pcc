from rtypes.pcc.utils.recursive_dictionary import OrderedSliceableDict, RecursiveDictionary

class TypeState(OrderedSliceableDict):
    """description of class"""
    @staticmethod
    def merge_state(states):
        return states

    def __init__(self, type_obj):
        self.tp_obj = type_obj
        self.transformations = dict()
        OrderedSliceableDict.__init__(self)

    def __getitem__(self, key):
        if isinstance(key, slice):
            initial = []
            if key.start in self.transformations:
                initial.extend(self.transformations[key.start])
            return initial + OrderedSliceableDict.__getitem__(self, key)[1:]
        return OrderedSliceableDict.__getitem__(self, key)

    def push_update(
            self, lastknown_stamp, current_stamp,
            alt_timestamp, update):
        next_adds = dict()
        # Add changes for this stamp
        for oid, obj_changes in update.iteritems():
            # Pick up all changes for this object since lastknown_stamp
            trans, lastknown_stamp = self.pickup_transforms(lastknown_stamp, oid)
            conflated = self.conflate_changes(trans + self[lastknown_stamp:][1:])
            next_update, new_trans = self.resolve_changes(
                conflated, update, lastknown_stamp, current_stamp, alt_timestamp)
            next_adds[oid] = (next_update, new_trans) 
        # Generate consequences as the next stamp
        for oid in next_adds:
            next_update, new_trans = next_adds[oid]
            if next_update:
                self[alt_timestamp] = next_update
            if new_trans:
                self.transformations.setdefault(current_timestamp, dict())[oid] = new_trans
    
    def pickup_transforms(self, lastknown_stamp, oid):
        return [change[oid] for change in self[lastknown_stamp:] if oid in change]

    def conflate_changes(self, changes):
        final_change = dict()
        for change in changes:
            final_change = self.merge_change(self.tp_obj.name, final_change, change)
        return final_change

    def merge_change(self, group_name, dest, src):
        if not dest:
            dest.update(src)
            return dest
        dest_event = dest["types"][group_name]
        src_event = src["types"][group_name] 
        if dest_event == Event.Delete:
            if src_event != Event.New:
                # If the previous record is Delete,
                # and the next record is not new,
                # delete the next record.
                # This case should not happen
                # unless src_event is Event.Modification
                return dest
            # This case could easily happen.
            # Do not return src directly, always a copy.
            return dict(src)
        if src_event == Event.Delete:
            return dict(src)
        if dest_event == Event.Modification and src_event == Event.New:
            # Error. This case is a merge conflict.
            # Should already have been taken care off.
            # How is it there?
            raise RuntimeError("Bad condition reached. Investigate")
        if src_event == Event.Modification:
            # Should be the only condition left to merge on.
            if "dims" not in src:
                # Nothing to merge actually.
                return dest
            dest.setdefault("dims", RecursiveDictionary()).update(src["dims"])
            # The type map should remain whatever dest type map is.
            # New should remain new, modification should remain modification.
            return dest
        raise RuntimeError("Why did it reach this line. It should not be here.")


class ObjState(RecursiveDictionary):
    def __hash__(self):
        return self.oid

    def __eq__(self, v):
        return self.oid == v.oid

    def __init__(self, type_obj, oid):
        self.tp_obj = type_obj
        self.oid = oid
        self.changes = RecursiveDictionary()

    def update_obj(self, update, startstamp, updatestamp):
        pass



