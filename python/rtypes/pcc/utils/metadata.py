from rtypes.pcc.attributes import rtype_property, aggregate_property, namespace_property
from rtypes.pcc.utils.enums import PCCCategories
from rtypes.pcc.this import thisclass, thisattr


class Metadata(object):
    def __repr__(self):
        return self.name

    @property
    def dimension_names(self):
        return [d._name for d in self.dimensions]

    @property
    def groupname(self):
        return self.group_type.name

    @property
    def parent_types(self):
        return [p.cls for p in self.base_parents]

    def __init__(self, cls, final_category, parents,
                 projection_dims=None, base_parents=None):
        self.name = cls.__module__ + "." + cls.__name__
        self.shortname = cls.__name__
        self.aliases = set([self.name])
        self.cls = cls
        self.type_pickle = None
        self.group_type = None
        self.categories = set()
        self.group_members = set()
        self.parameter_types = dict()
        self.dimensions = set()
        self.group_dimensions = set()
        self.namespace_dimensions = list()
        self.namespaces = dict()
        self.primarykey = None
        self.predicate = None
        self.limit = None
        self.group_by = None
        self.distinct = None
        self.sort_by = None
        self.projection_dims = set(
            projection_dims) if projection_dims else set()
        self.final_category = final_category
        self.categories.add(final_category)
        self.category_execution_order = list()
        if Metadata.processable_category(final_category):
            self.category_execution_order.append(final_category)

        self.parents = list()
        for p in parents:
            if not hasattr(p, "__rtypes_metadata__"):
                raise TypeError(
                    "Type {0} does not have a PCC parent ({1})".format(
                        self.name, repr(p)))
            self.parents.append(p.__rtypes_metadata__)
        self.build_required_attrs()
        self.dimension_map = self.rebuild_dimension_map()
        if self.distinct or self.limit or self.group_by or self.sort_by:
            self.categories.add(PCCCategories.impure)
        self.base_parents = base_parents if base_parents else self.parents

    @staticmethod
    def processable_category(category):
        return category in set(
            [PCCCategories.intersection, PCCCategories.join,
             PCCCategories.pcc_set, PCCCategories.projection,
             PCCCategories.subset, PCCCategories.union])

    @staticmethod
    def get_properties(cls):
        dimensions = set()
        group_dimensions = set()
        namespace_dimensions = list()
        namespaces = dict()
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
                    if primarykey != None and primarykey != attr_prop:
                        raise TypeError(
                            "Class {0} has more than one primary key".format(
                                repr(cls)))
                    primarykey = attr_prop
            elif isinstance(attr_prop, aggregate_property):
                group_dimensions.add(attr_prop)
            elif isinstance(attr_prop, namespace_property):
                namespaces[attr_prop._name] = Metadata.get_properties(
                    attr_prop._type)
                namespace_dimensions.append(attr_prop)
        return (
            primarykey, dimensions, group_dimensions,
            namespace_dimensions, namespaces)

    @staticmethod
    def get_dim_map(
            dimensions, group_dimensions, namespace_dimensions, namespaces):
        dim_map = dict()
        for d in dimensions:
            dim_map[d._name] = d
        for d in group_dimensions:
            dim_map[d._name] = d
        for d in namespace_dimensions:
            dim_map[d._name] = d
        for ns in namespaces:
            ns_dim_map = Metadata.get_dim_map(*(namespaces[ns][1:]))
            for name in ns_dim_map:
                dim_map[ns + "." + name] = ns_dim_map[name]
        return dim_map

    def rebuild_dimension_map(self):
        return Metadata.get_dim_map(
            self.dimensions, self.group_dimensions,
            self.namespace_dimensions, self.namespaces)

    def check_pkey_is_group_pkey(self, pkey):
        if pkey is not None and pkey != self.group_type.primarykey:
            raise TypeError(
                "Subset class {0} does not share the primary key "
                "with the parent class {1}".format(
                    self.name, self.group_type.name))

    def parse_group_dims_as_subset(self, pkey, dims, group_dims, namespaces):
        self.check_pkey_is_group_pkey(pkey)
        pkey = self.group_type.primarykey
        dims = dims.union(self.parents[0].dimensions)
        group_dims = group_dims.union(self.parents[0].group_dimensions)
        return pkey, dims, group_dims, namespaces

    def parse_group_dims_as_join(self, pkey, dims, group_dims, namespaces):
        return pkey, dims, group_dims, namespaces

    def parse_group_dims_as_projection(
            self, pkey, dims, group_dims, namespaces):
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
        namespaces = set(dim for dim in self.projection_dims
                         if isinstance(dim, namespace_property))
        return pkey, dims, group_dims, namespaces

    def parse_group_dims_as_union_or_intersection(
            self, pkey, dims, group_dims, namespaces):
        if len(set(p.primarykey for p in self.parents)) > 1:
            raise TypeError(
                "Union class {0} requires that all participating classes have "
                "the same name for the primary key".format(self.name))

        pkey = self.parents[0].primarykey
        dims = set()
        group_dims = set()
        namespaces = dict()
        for p in self.parents:
            if dims:
                dims.intersection_update(p.dimensions)
            else:
                dims = set(p.dimensions)
            if group_dims:
                group_dims.intersection_update(p.group_dimensions)
            else:
                group_dims = set(p.group_dimensions)
            for ns in p.namespaces:
                namespaces.setdefault(
                    ns, p.namespaces[ns]).intersection_update(
                        p.namespaces[ns])

        return pkey, dims, group_dims, namespaces

    def parse_dimensions(self):
        pkey, dims, group_dims, namespace_dimensions, namespaces = (
            Metadata.get_properties(self.cls))
        if self.final_category == PCCCategories.subset:
            pkey, dims, group_dims, namespaces = (
                self.parse_group_dims_as_subset(
                    pkey, dims, group_dims, namespaces))
        elif self.final_category == PCCCategories.join:
            pkey, dims, group_dims, namespaces = self.parse_group_dims_as_join(
                pkey, dims, group_dims, namespaces)
        elif self.final_category == PCCCategories.projection:
            pkey, dims, group_dims, namespaces = (
                self.parse_group_dims_as_projection(
                    pkey, dims, group_dims, namespaces))
        elif (self.final_category == PCCCategories.union
              or self.final_category == PCCCategories.intersection):
            pkey, dims, group_dims, namespaces = (
                self.parse_group_dims_as_union_or_intersection(
                    pkey, dims, group_dims, namespaces))
        self.primarykey = pkey
        self.dimensions = dims
        self.group_dimensions = group_dims
        self.namespaces = namespaces
        self.namespace_dimensions = namespace_dimensions
        setattr(self.cls, self.primarykey._name, self.primarykey)
        setattr(self.cls, "__primarykey__", self.primarykey)
        for d in self.dimensions.union(self.group_dimensions).union(
                self.projection_dims):
            setattr(self.cls, d._name, d)
        if hasattr(self.cls, "__predicate__"):
            self.predicate = self.cls.__predicate__
        if hasattr(self.cls, "__distinct__"):
            self.distinct = self.cls.__distinct__
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
            self.group_type = self.parents[0].group_type
            self.group_members = self.group_type.group_members
        self.group_members.add(self)

    def build_required_attrs(self):
        self.parse_dependencies()
        self.parse_dimensions()

    def resolve_anon_class(self, parents):
        return [self.cls if isinstance(p, thisclass) else p
                for p in parents]

    def resolve_anon_dimensions(self, dimensions):
        resolved_dims = set()
        for d in dimensions:
            if isinstance(d, thisattr):
                dim_map = self.dimension_map
                if d.__rtypes_attr_name__ not in dim_map:
                    raise TypeError(
                        "Class {0} does not have dimension {1}".format(
                            self.name, d.__rtypes_attr_name__))
                resolved_dims.add(dim_map[d.__rtypes_attr_name__])
            else:
                resolved_dims.add(d)
        return resolved_dims

    def add_data(
            self, cls, final_category, parents, projection_dims=None):
        if final_category in set(
                [PCCCategories.join, PCCCategories.union,
                 PCCCategories.intersection, PCCCategories.pcc_set]):
            raise TypeError(
                "Cannot compose join, union, intersection "
                "or pcc_set onto existing PCC Categories "
                "in class {0}".format(self.name))
        parents = self.resolve_anon_class(parents)
        projection_dims = self.resolve_anon_dimensions(projection_dims)
        old_categories = self.categories
        old_category_order = self.category_execution_order
        new_metadata = Metadata(
            cls, final_category, parents,
            projection_dims, base_parents=self.base_parents)
        new_metadata.categories.update(old_categories)
        new_metadata.category_execution_order = old_category_order + (
            [final_category]
            if Metadata.processable_category(final_category) else
            [])
        return new_metadata
