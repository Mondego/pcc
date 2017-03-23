from enums import ObjectType
from multiprocessing import RLock

type_lock = RLock()
object_lock = RLock()
class DataframeType(object):
    # Name -> str, 
    # Type -> type, 
    # GroupType -> type, 
    # GroupKey -> key, 
    # Category -> dict, 
    # ClosestSaveableParent -> DataframeType, 
    # GroupMembers -> [DataframeType]
    # IsPure -> Bool

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, obj):
        if type(obj) == str or type(obj) == unicode:
            return self.name == obj
        return self.name == obj.name

    #@property
    #def name(self): 
    #    return self.type.__realname__

    #@property
    #def group_key(self): 
    #    return self.group_type.__realname__

    @property
    def can_be_persistent(self): 
        return self.saveable_parent != None

    @property
    def has_params(self):
        return len(self.parameter_types) != 0

    #@property
    #def is_base_type(self):
    #    return self.name == self.group_key

    #@property
    #def is_projection(self):
    #    return ObjectType.Projection in self.categories

    def __init__(self, 
                 tp, 
                 grp_tp, 
                 categories, 
                 depends = list(),
                 saveable_parent = None, 
                 group_members = set(),
                 pure_group_members = set(),
                 is_pure = False,
                 parameter_types = dict(),
                 super_class = None,
                 observable = True):
        self.type = tp
        self.group_type = grp_tp
        self.categories = categories
        self.depends = depends
        self.saveable_parent = saveable_parent
        self.group_members = group_members
        self.pure_group_members = pure_group_members
        self.is_pure = is_pure
        self.parameter_types = parameter_types
        self.super_class = super_class
        self.observable = observable
        self.name = tp.__realname__
        self.group_key = self.group_type.__realname__
        self.is_base_type = self.name == self.group_key
        self.is_projection = ObjectType.Projection in self.categories

