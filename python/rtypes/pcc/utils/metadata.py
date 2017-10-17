from rtypes.pcc.attributes import rtype_property, aggregate_property
from rtypes.pcc.utils.pcc_categories import PCCCategories
# from rtypes.pcc import 


class Metadata(object):
    def __repr__(self):
        return self.name

    @property
    def dimension_names(self):
        return [d._name for d in self.dimensions]

    @property
    def dimension_map(self):
        return {d._name: d for d in self.dimensions}

    @property
    def group_name(self):
        return self.group_type.name

    def __init__(self, cls, final_category, parents, projection_dims = None):
        self.name = cls.__module__ + "." + cls.__name__
        self.aliases = set([self.name])
        self.cls = cls
        self.type_pickle = None
        self.group_type = None
        self.categories = None
        self.group_members = set()
        self.parameter_types = list()
        self.dimensions = list()
        self.group_dimensions = list()
        self.primarykey = None
        self.predicate = None
        self.limit = None
        self.group_by = None
        self.distinct = None
        self.sort_by = None
        self.projection_dims = set(
            projection_dims) if projection_dims else set()
        self.final_category = final_category
        self.parents = set()
        for p in parents:
            if not hasattr(p, "__rtypes_metadata__"):
                raise TypeError(
                    "Type {0} does not have a PCC parent ({1})".format(
                        self.name, repr(p)))
            self.parents.add(p.__rtypes_metadata__)
        self.build_required_attrs()

    @staticmethod
    def get_properties(cls):
        dimensions = set()
        group_dimensions = set()
        primarykey = None
        for attr in dir(cls):
            try:
                attr_prop = getattr(cls, attr)
            except AttributeError:
                continue
            if isinstance(attr_prop, rtype_property):
                dimensions.add(attr_prop)
                if (hasattr(attr_prop, "_primarykey")
                    and attr_prop._primarykey != None):
                    if primarykey != None:
                        raise TyperError(
                            "Class {0} has more than one primary key".format(
                                repr(cls)))
                    primarykey = attr_prop
            elif isinstance(attr_prop, aggregate_property):
                group_dimensions.add(attr_prop)
        return primarykey, dimensions, group_dimensions
        
    def check_pkey_is_group_pkey(self, pkey):
        if pkey is not None and pkey != self.group_type.primarykey:
            raise TypeError(
                "Subset class {0} does not share the primary key "
                "with the parent class {1}".format(
                    self.name, self.group_type.name))

    def parse_group_dims_as_subset(self, pkey, dims, group_dims):
        self.check_pkey_is_group_pkey(pkey)
        pkey = self.group_type.primarykey
        dims = dims.union(list(self.parents)[0].dimensions)
        group_dims = group_dims.union(list(self.parents)[0].group_dimensions)
        return pkey, dims, group_dims 

    def parse_group_dims_as_join(self, pkey, dims, group_dims):
        return pkey, dims, group_dims 

    def parse_group_dims_as_projection(self, pkey, dims, group_dims):
        self.check_pkey_is_group_pkey(pkey)
        if self.group_type.primarykey not in self.projection_dims:
            raise TypeError(
                "Projection class {0} requires "
                "a primary key from the class it projects".format(self.name))
            
        pkey = self.group_type.primarykey
        dims = set(dim for dim in self.projection_dims
                   if isinstance(dim, rtype_property))
        group_dims = set(dim for dim in self.projection_dims
                         if isinstance(dim, aggregate_property))
        return pkey, dims, group_dims

    def parse_group_dims_as_union_or_intersection(
        self, pkey, dims, group_dims):
        if len(set(p.primarykey for p in self.parents)) > 1:
            raise TypeError(
                "Union class {0} requires that all participating classes have "
                "the same name for the primary key".format(self.name))

        pkey = list(self.parents)[0].primarykey
        dims = set()
        group_dims = set
        for p in self.parents:
            if dims:
                dims.intersection_update(p.dimensions)
            else:
                dims = set(p.dimensions)
            if group_dims:
                group_dims.intersection_update(p.group_dimensions)
            else:
                group_dims = set(p.group_dimensions)
        return pkey, dims, group_dims
    
    def parse_dimensions(self):
        pkey, dims, group_dims = Metadata.get_properties(self.cls)
        if self.final_category == PCCCategories.subset:
            pkey, dims, group_dims = self.parse_group_dims_as_subset(
                pkey, dims, group_dims)
        elif self.final_category == PCCCategories.join:
            pkey, dims, group_dims = self.parse_group_dims_as_join(
                pkey, dims, group_dims)
        elif self.final_category == PCCCategories.projection:
            pkey, dims, group_dims = self.parse_group_dims_as_projection(
                pkey, dims, group_dims)
        elif (self.final_category == PCCCategories.union
              or self.final_category == PCCCategories.intersection):
            pkey, dims, group_dims = (
                self.parse_group_dims_as_union_or_intersection(
                    pkey, dims, group_dims))
        self.primarykey = pkey
        self.dimensions = dims
        self.group_dimensions = group_dims
        setattr(self.cls, self.primarykey._name, self.primarykey)
        for d in self.dimensions.union(self.group_dimensions).union(
            self.projection_dims):
            setattr(self.cls, d._name, d)
        if hasattr(self.cls, "__predicate__"):
            self.predicate = self.cls.__predicate__
        if hasattr(self.cls, "__limit__"):
            self.limit = self.cls.__limit__
        if hasattr(self.cls, "__group_by__"):
            self.group_by = self.cls.__group_by__
        if hasattr(self.cls, "__order_by__"):
            self.sort_by = self.cls.__sort_by__        


    def parse_dependencies(self):
        if self.final_category in set(
          [PCCCategories.pcc_set, PCCCategories.join,
           PCCCategories.union, PCCCategories.intersection]):
            self.group_type = self
        elif self.final_category in set(
          [PCCCategories.subset, PCCCategories.projection]):
            self.group_type = list(self.parents)[0].group_type
            self.group_members = self.group_type.group_members
        self.group_members.add(self)

    def build_required_attrs(self):
        self.parse_dependencies()
        self.parse_dimensions()

    def resolve_anon_class(self, parents):
        return [self if isinstance(p, AnonClass) and AnonClass.this else p
                for p in parents]

    def resolve_anon_dimensions(self, dimensions):
        resolved_dims = set()
        for d in dimensions:
            if isinstance(d, AnonDimensions):
                dim_map = self.dimension_map
                if d.name not in dim_map:
                    raise TypeError(
                        "Class {0} does not have dimension {1}".format(
                            self.name, d.name))
                resolved_dims.add(dim_map[d.name])
            else:
                resolved_dims.add(d)
        return resolved_dims
    
    def add_data(
        self, cls, final_category, parents, projection_dims = None):
        if final_category in set(
            [PCCCategories.join, PCCCategories.union,
             PCCCategories.intersection, PCCCategories.pcc_set]):
            raise TypeError(
                "Cannot compose join, union, intersection "
                "or pcc_set onto existing PCC Categories "
                "in class {0}".format(self.name))
        parents = self.resolve_anon_class(parents)
        projection_dims = self.resolve_anon_dimensions(projection_dims)
        new_metadata = Metadata(cls, final_category, parents, projection_dims)
        return new_metadata            
