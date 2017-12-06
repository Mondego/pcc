from tests import test_classes as tc
from rtypes.pcc.utils.metadata import Metadata
from rtypes.pcc.utils.enums import PCCCategories
from rtypes.pcc import create

import unittest, json

class test_create(unittest.TestCase):
    def setUp(self):
        np1 = tc.NonPCC()
        np2 = tc.NonPCC()

        s1 = tc.SmallBase()
        s1.oid = "sb1"
        s1.sprop1 = "base"

        s2 = tc.SmallBase()
        s2.oid = "b1"
        s2.sprop1 = "subset"

        s3 = tc.SmallBase()
        s3.oid = "sb2"
        s3.sprop1 = "subset"
        
        l1 = tc.LargeBase()
        l1.oid = "b1"
        l1.prop1 = "subset"
        l1.prop2 = ["item1", "item2"]
        l1.prop3 = np1
        l1.prop4 = s1
        
        l2 = tc.LargeBase()
        l2.oid = "lb2"
        l2.prop1 = "normal"
        l2.prop2 = ["item3", "item4"]
        l2.prop3 = np1
        l2.prop4 = s1
        
        l3 = tc.LargeBase()
        l3.oid = "lb3"
        l3.prop1 = "normal"
        l3.prop2 = ["item5", "item6"]
        l3.prop3 = np2
        l3.prop4 = s3

        self.small_bases = [s1, s2, s3]
        self.large_bases = [l1, l2, l3]
        self.s1, self.s2, self.s3, self.l1, self.l2, self.l3 = (
            s1, s2, s3, l1, l2, l3)
        self.np1 = np1
        self.np2 = np2
        
    def test_basic_create(self):
        self.assertListEqual(
            [self.s1, self.s2, self.s3], create(tc.SmallBase, self.small_bases))
        self.assertListEqual(
            [self.l1, self.l2, self.l3], create(tc.LargeBase, self.large_bases))

    def test_subset_create1(self):
        subset_list = create(tc.SubsetLargeBase, self.large_bases)
        self.assertEqual(1, len(subset_list))
        subl1 = subset_list[0]
        self.assertEqual("b1", subl1.oid)
        self.assertEqual("subset", subl1.prop1)
        self.assertListEqual(["item1", "item2"], subl1.prop2)
        self.assertEqual(self.np1, subl1.prop3)
        self.assertEqual(self.s1, subl1.prop4)
        self.assertEqual(tc.SubsetLargeBase, type(subl1))
        self.assertListEqual(["item1", "item2"], subl1.func2())
        self.assertFalse(hasattr(subl1, "func1"))

    def test_subset_create2(self):
        subset_list = create(tc.InheritedSubsetLargeBase, self.large_bases)
        self.assertEqual(1, len(subset_list))
        subl1 = subset_list[0]
        self.assertEqual("b1", subl1.oid)
        self.assertEqual("subset", subl1.prop1)
        self.assertListEqual(["item1", "item2"], subl1.prop2)
        self.assertEqual(self.np1, subl1.prop3)
        self.assertEqual(self.s1, subl1.prop4)
        self.assertEqual(tc.InheritedSubsetLargeBase, type(subl1))
        self.assertListEqual(["item1", "item2"], subl1.func2())
        self.assertTrue(hasattr(subl1, "func1"))

    def test_projection_create1(self):
        project_list = create(tc.ProjectLargeBase, self.large_bases)
        self.assertEqual(3, len(project_list))
        pl1 = project_list[0]
        self.assertEqual(tc.ProjectLargeBase, type(pl1))
        self.assertEqual("b1", pl1.oid)
        self.assertEqual("subset", pl1.prop1)
        self.assertTrue(hasattr(pl1, "prop2"))
        self.assertFalse(hasattr(pl1, "prop3"))
        self.assertFalse(hasattr(pl1, "prop4"))

        pl2 = project_list[1]
        self.assertEqual(tc.ProjectLargeBase, type(pl2))
        self.assertEqual("lb2", pl2.oid)
        self.assertEqual("normal", pl2.prop1)
        self.assertTrue(hasattr(pl2, "prop2"))
        self.assertFalse(hasattr(pl2, "prop3"))
        self.assertFalse(hasattr(pl2, "prop4"))

        pl3 = project_list[2]
        self.assertEqual(tc.ProjectLargeBase, type(pl3))
        self.assertEqual("lb3", pl3.oid)
        self.assertEqual("normal", pl3.prop1)
        self.assertTrue(hasattr(pl3, "prop2"))
        self.assertFalse(hasattr(pl3, "prop3"))
        self.assertFalse(hasattr(pl3, "prop4"))

    def test_join_create1(self):
        join_list = create(tc.JoinSmallAndLargeBase, self.small_bases, self.large_bases)
        self.assertEqual(1, len(join_list))
        jl1 = join_list[0]
        self.assertEqual(tc.JoinSmallAndLargeBase, type(jl1))
        self.assertEqual("b1", jl1.oid)
        self.assertEqual("subset", jl1.sb.sprop1)
        self.assertEqual("subset", jl1.lb.prop1)
        self.assertTrue(hasattr(jl1.lb, "prop2"))
        self.assertTrue(hasattr(jl1.lb, "prop3"))
        self.assertTrue(hasattr(jl1.lb, "prop4"))
