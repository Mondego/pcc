from tests import test_classes as tc
from rtypes.pcc.utils.metadata import Metadata
from rtypes.pcc.utils.enums import PCCCategories

import unittest

class test_metadata(unittest.TestCase):
    def test_basic_classes(self):
        self.assertTrue(hasattr(tc.SmallBase, "__rtypes_metadata__"))
        self.assertEqual(Metadata, type(tc.SmallBase.__rtypes_metadata__))
        self.assertTrue(hasattr(tc.LargeBase, "__rtypes_metadata__"))
        self.assertEqual(Metadata, type(tc.LargeBase.__rtypes_metadata__))
        self.assertTrue(hasattr(tc.InheritedSmallBase, "__rtypes_metadata__"))
        self.assertEqual(
            Metadata, type(tc.InheritedSmallBase.__rtypes_metadata__))
        self.assertTrue(
            hasattr(tc.InheritedSubsetLargeBase, "__rtypes_metadata__"))
        self.assertEqual(
            Metadata, type(tc.InheritedSubsetLargeBase.__rtypes_metadata__))
        self.assertTrue(
            hasattr(tc.NonPCCInheritedSmallBase, "__rtypes_metadata__"))
        self.assertEqual(
            Metadata, type(tc.NonPCCInheritedSmallBase.__rtypes_metadata__))
        self.assertTrue(hasattr(tc.ProjectLargeBase, "__rtypes_metadata__"))
        self.assertEqual(
            Metadata, type(tc.ProjectLargeBase.__rtypes_metadata__))
        self.assertTrue(hasattr(tc.SubsetLargeBase, "__rtypes_metadata__"))
        self.assertEqual(
            Metadata, type(tc.SubsetLargeBase.__rtypes_metadata__))
        self.assertFalse(hasattr(tc.NonPCC, "__rtypes_metadata__"))

    def test_small_base(self):
        small_meta = tc.SmallBase.__rtypes_metadata__
        self.assertEqual("tests.test_classes.SmallBase", small_meta.name)
        self.assertEqual("tests.test_classes.SmallBase", small_meta.groupname)
        self.assertEqual(small_meta, small_meta.group_type)
        self.assertListEqual(list(), small_meta.parents)
        self.assertEqual(set([small_meta]), small_meta.group_members)
        self.assertDictEqual(dict(), small_meta.parameter_types)
        self.assertEqual(tc.SmallBase.oid, small_meta.primarykey)
        self.assertEqual(None, small_meta.predicate)
        self.assertEqual(None, small_meta.limit)
        self.assertEqual(None, small_meta.group_by)
        self.assertEqual(None, small_meta.distinct)
        self.assertEqual(None, small_meta.sort_by)
        self.assertEqual(set(), small_meta.projection_dims)
        self.assertEqual(PCCCategories.pcc_set, small_meta.final_category)


    def test_large_base(self):
        large_meta = tc.LargeBase.__rtypes_metadata__
        self.assertEqual(
            "tests.test_classes.LargeBase", large_meta.name)
        self.assertEqual(
            "tests.test_classes.LargeBase", large_meta.groupname)
        self.assertEqual(large_meta, large_meta.group_type)
        self.assertListEqual(list(), large_meta.parents)
        self.assertEqual(
            set([large_meta,
                 tc.InheritedSubsetLargeBase.__rtypes_metadata__,
                 tc.SubsetLargeBase.__rtypes_metadata__,
                 tc.ProjectLargeBase.__rtypes_metadata__]),
            large_meta.group_members)
        self.assertDictEqual(dict(), large_meta.parameter_types)
        self.assertEqual(tc.LargeBase.oid, large_meta.primarykey)
        self.assertEqual(None, large_meta.predicate)
        self.assertEqual(None, large_meta.limit)
        self.assertEqual(None, large_meta.group_by)
        self.assertEqual(None, large_meta.distinct)
        self.assertEqual(None, large_meta.sort_by)
        self.assertEqual(set(), large_meta.projection_dims)
        self.assertEqual(
            PCCCategories.pcc_set, large_meta.final_category)
        self.assertTrue(hasattr(tc.LargeBase, "func1"))

    def test_inherited_small_base(self):
        self.assertEqual(
            tc.SmallBase.__rtypes_metadata__,
            tc.InheritedSmallBase.__rtypes_metadata__)

    def test_nonpcc_inherited_small_base(self):
        self.assertEqual(
            tc.SmallBase.__rtypes_metadata__,
            tc.NonPCCInheritedSmallBase.__rtypes_metadata__)

    def test_subset_large_base(self):
        subset_large_meta = tc.SubsetLargeBase.__rtypes_metadata__
        large_meta = tc.LargeBase.__rtypes_metadata__
        self.assertEqual(
            "tests.test_classes.SubsetLargeBase", subset_large_meta.name)
        self.assertEqual(
            "tests.test_classes.LargeBase", subset_large_meta.groupname)
        self.assertEqual(large_meta, subset_large_meta.group_type)
        self.assertListEqual([large_meta], subset_large_meta.parents)
        self.assertEqual(
            set([tc.LargeBase.__rtypes_metadata__,
                 subset_large_meta,
                 tc.InheritedSubsetLargeBase.__rtypes_metadata__,
                 tc.SubsetLargeBase.__rtypes_metadata__,
                 tc.ProjectLargeBase.__rtypes_metadata__]),
            subset_large_meta.group_members)
        self.assertDictEqual(dict(), subset_large_meta.parameter_types)
        self.assertEqual(tc.SubsetLargeBase.oid, subset_large_meta.primarykey)
        self.assertEqual(
            tc.SubsetLargeBase.__predicate__, subset_large_meta.predicate)
        self.assertEqual(None, subset_large_meta.limit)
        self.assertEqual(None, subset_large_meta.group_by)
        self.assertEqual(None, subset_large_meta.distinct)
        self.assertEqual(None, subset_large_meta.sort_by)
        self.assertEqual(set(), subset_large_meta.projection_dims)
        self.assertEqual(
            PCCCategories.subset, subset_large_meta.final_category)
        self.assertTrue(hasattr(tc.SubsetLargeBase, "func2"))
        self.assertFalse(hasattr(tc.SubsetLargeBase, "func1"))
        self.assertTrue(hasattr(tc.SubsetLargeBase, "oid"))
        self.assertTrue(hasattr(tc.SubsetLargeBase, "prop1"))
        self.assertTrue(hasattr(tc.SubsetLargeBase, "prop2"))
        self.assertTrue(hasattr(tc.SubsetLargeBase, "prop3"))
        self.assertTrue(hasattr(tc.SubsetLargeBase, "prop4"))

    def test_inherited_subset_large_base(self):
        subset_large_meta = tc.InheritedSubsetLargeBase.__rtypes_metadata__
        large_meta = tc.LargeBase.__rtypes_metadata__
        self.assertEqual(
            "tests.test_classes.InheritedSubsetLargeBase",
            subset_large_meta.name)
        self.assertEqual(
            "tests.test_classes.LargeBase", subset_large_meta.groupname)
        self.assertEqual(large_meta, subset_large_meta.group_type)
        self.assertListEqual([large_meta], subset_large_meta.parents)
        self.assertEqual(
            set([tc.LargeBase.__rtypes_metadata__,
                 subset_large_meta,
                 tc.InheritedSubsetLargeBase.__rtypes_metadata__,
                 tc.SubsetLargeBase.__rtypes_metadata__,
                 tc.ProjectLargeBase.__rtypes_metadata__]),
            subset_large_meta.group_members)
        self.assertDictEqual(dict(), subset_large_meta.parameter_types)
        self.assertEqual(
            tc.InheritedSubsetLargeBase.oid, subset_large_meta.primarykey)
        self.assertEqual(
            tc.InheritedSubsetLargeBase.__predicate__,
            subset_large_meta.predicate)
        self.assertEqual(None, subset_large_meta.limit)
        self.assertEqual(None, subset_large_meta.group_by)
        self.assertEqual(None, subset_large_meta.distinct)
        self.assertEqual(None, subset_large_meta.sort_by)
        self.assertEqual(set(), subset_large_meta.projection_dims)
        self.assertEqual(
            PCCCategories.subset, subset_large_meta.final_category)
        self.assertTrue(hasattr(tc.InheritedSubsetLargeBase, "func2"))
        self.assertTrue(hasattr(tc.InheritedSubsetLargeBase, "func1"))
        self.assertTrue(hasattr(tc.InheritedSubsetLargeBase, "oid"))
        self.assertTrue(hasattr(tc.InheritedSubsetLargeBase, "prop1"))
        self.assertTrue(hasattr(tc.InheritedSubsetLargeBase, "prop2"))
        self.assertTrue(hasattr(tc.InheritedSubsetLargeBase, "prop3"))
        self.assertTrue(hasattr(tc.InheritedSubsetLargeBase, "prop4"))

    def test_project_large_base(self):
        project_large_meta = tc.ProjectLargeBase.__rtypes_metadata__
        large_meta = tc.LargeBase.__rtypes_metadata__
        self.assertEqual(
            "tests.test_classes.ProjectLargeBase",
            project_large_meta.name)
        self.assertEqual(large_meta, project_large_meta.group_type)
        self.assertListEqual([large_meta], project_large_meta.parents)
        self.assertEqual(
            set([tc.LargeBase.__rtypes_metadata__,
                 project_large_meta,
                 tc.InheritedSubsetLargeBase.__rtypes_metadata__,
                 tc.SubsetLargeBase.__rtypes_metadata__,
                 tc.ProjectLargeBase.__rtypes_metadata__]),
            project_large_meta.group_members)
        self.assertDictEqual(dict(), project_large_meta.parameter_types)
        self.assertEqual(
            tc.ProjectLargeBase.oid, project_large_meta.primarykey)
        self.assertEqual(None, project_large_meta.limit)
        self.assertEqual(None, project_large_meta.group_by)
        self.assertEqual(None, project_large_meta.distinct)
        self.assertEqual(None, project_large_meta.sort_by)
        self.assertEqual(set([
            tc.ProjectLargeBase.oid, tc.ProjectLargeBase.prop1,
            tc.ProjectLargeBase.prop2]), project_large_meta.projection_dims)
        self.assertEqual(
            PCCCategories.projection, project_large_meta.final_category)
        self.assertTrue(hasattr(tc.ProjectLargeBase, "func2"))
        self.assertFalse(hasattr(tc.ProjectLargeBase, "func1"))
        self.assertTrue(hasattr(tc.ProjectLargeBase, "oid"))
        self.assertTrue(hasattr(tc.ProjectLargeBase, "prop1"))
        self.assertTrue(hasattr(tc.ProjectLargeBase, "prop2"))
        self.assertFalse(hasattr(tc.ProjectLargeBase, "prop3"))
        self.assertFalse(hasattr(tc.ProjectLargeBase, "prop4"))
