import unittest, json

from tests import test_classes as tc
from rtypes.dataframe.objectless_dataframe import ObjectlessDataframe
from rtypes.dataframe.dataframe_client import dataframe_client
from rtypes.pcc.utils.enums import Record, Event
import cProfile

class test_objectless_dataframe(unittest.TestCase):
    @staticmethod
    def apply_versions(gpname, changes, version_map, new_version):
        version_for_tp = version_map.setdefault(gpname, dict())
        for oid, obchanges in changes["gc"][gpname].iteritems():
            old_version = version_for_tp[oid] if oid in version_for_tp else None
            version_for_tp[oid] = new_version
            obchanges["version"] = [old_version, new_version]

    @staticmethod
    def get_versions(gpname, changes, version_map, tpname=None):
        if not tpname:
            tpname = gpname
        for oid, obchanges in changes["gc"][gpname].iteritems():
            if obchanges["types"][tpname] == Event.Delete:
                del version_map[tpname][oid]
            else:
                version_map[tpname][oid] = obchanges["version"][1]


    def test_basic_apply_no_maintain(self):
        self.maxDiff = None
        df = ObjectlessDataframe(maintain_change_record=False)
        df.add_type(tc.SmallBase)
        changes1 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "oid": {
                                "type": Record.STRING,
                                "value": "1"
                            },
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABC"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.New
                        },
                        "version": [None, 0]
                    }
                }
            }
        }
        df.apply_changes(changes1)
        record1 = df.get_record(
            {"tests.test_classes.SmallBase": dict()})
        self.assertDictEqual(changes1, record1)
        record2 = df.get_record({"tests.test_classes.SmallBase": {
            "1": 0
        }})
        self.assertDictEqual({"gc": dict()}, record2)
        changes2 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABCDEF"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [0, 1]
                    }
                }
            }
        }
        df.apply_changes(changes2)
        record3 = df.get_record(
            {"tests.test_classes.SmallBase": dict()})
        self.assertDictEqual({
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "oid": {
                                "type": Record.STRING,
                                "value": "1"
                            },
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABCDEF"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.New
                        },
                        "version": [None, 1]
                    }
                }
            }
        }, record3)
        record4 = df.get_record({"tests.test_classes.SmallBase": {
            "1": 0
        }})
        self.assertDictEqual(changes2, record4)
        changes3 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "AB"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [0, 10]
                    }
                }
            }
        }
        df.apply_changes(changes3)

        record5 = df.get_record(
            {"tests.test_classes.SmallBase": dict()})
        new_version = (
            record5["gc"]["tests.test_classes.SmallBase"]["1"]["version"][1])

        record6 = df.get_record({"tests.test_classes.SmallBase": {
            "1": 1
        }})

        self.assertDictEqual({
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "AB"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [1, new_version]
                    }
                }
            }
        }, record6)

        record7 = df.get_record({"tests.test_classes.SmallBase": {
            "1": 10
        }})

        self.assertDictEqual({
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [10, new_version]
                    }
                }
            }
        }, record7)

    def test_basic_apply_maintain1(self):
        self.maxDiff = None
        df = ObjectlessDataframe(maintain_change_record=True)
        df.add_type(tc.SmallBase)
        record0 = df.get_record(
            {"tests.test_classes.SmallBase": dict()}, app="APP2")
        self.assertDictEqual({"gc": dict()}, record0)

        changes1 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "oid": {
                                "type": Record.STRING,
                                "value": "1"
                            },
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABC"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.New
                        },
                        "version": [None, 0]
                    }
                }
            }
        }

        df.apply_changes(changes1, except_app="APP1")
        record1 = df.get_record(
            {"tests.test_classes.SmallBase": dict()}, app="APP2")
        self.assertDictEqual(changes1, record1)
        #APP1: 0, APP2: 0
        record = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 0}}, app="APP2")
        self.assertDictEqual({"gc": dict()}, record)

        changes2 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABCDEF"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [0, 1]
                    }
                }
            }
        }

        df.apply_changes(changes2, except_app="APP1")
        changes3 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABCDEFGHI"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [1, 2]
                    }
                }
            }
        }
        df.apply_changes(changes3, except_app="APP1")
        obj_state = df.state_manager.type_to_obj_dimstate[
            "tests.test_classes.SmallBase"].obj_to_state["1"].changes
        self.assertDictEqual({
            0: {
                "version": 0,
                "changes": {
                    "dims": {
                        "oid": {
                            "type": Record.STRING,
                            "value": "1"
                        },
                        "sprop1": {
                            "type": Record.STRING,
                            "value": "ABC"
                        }
                    }
                },
                "prev_version": None,
                "next_version": 2
            },
            2: {
                "version": 2,
                "changes": {
                    "dims": {
                        "sprop1": {
                            "type": Record.STRING,
                            "value": "ABCDEFGHI"
                        }
                    }
                },
                "prev_version": 0,
                "next_version": None
            }}, obj_state)

        record2 = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 0}}, app="APP2")
        self.assertDictEqual({
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABCDEFGHI"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [0, 2]
                    }
                }
            }
        }, record2)
        record3 = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 2}}, app="APP2")
        self.assertDictEqual({"gc": dict()}, record3)


    def test_basic_apply_maintain2(self):
        self.maxDiff = None
        df = ObjectlessDataframe(maintain_change_record=True)
        df.add_type(tc.SmallBase)
        changes1 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "oid": {
                                "type": Record.STRING,
                                "value": "1"
                            },
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABC"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.New
                        },
                        "version": [None, 0]
                    }
                }
            }
        }

        df.apply_changes(changes1, except_app="APP1")
        record1 = df.get_record(
            {"tests.test_classes.SmallBase": dict()}, app="APP2")
        self.assertDictEqual(changes1, record1)
        #APP1: 0, APP2: 0

        changes2 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABCDEF"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [0, 1]
                    }
                }
            }
        }

        df.apply_changes(changes2, except_app="APP1")
        record2 = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 0}}, app="APP2")
        self.assertDictEqual(changes2, record2)

        record3 = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 1}}, app="APP2")
        self.assertDictEqual({"gc": dict()}, record3)

        record4 = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 1}}, app="APP2")
        self.assertDictEqual({"gc": dict()}, record4)

        changes3 = {
            "gc": {
                "tests.test_classes.SmallBase": {
                    "1": {
                        "dims": {
                            "sprop1": {
                                "type": Record.STRING,
                                "value": "ABCDEFGHI"
                            }
                        },
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [1, 2]
                    }
                }
            }
        }
        df.apply_changes(changes3, except_app="APP1")

        record5 = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 1}}, app="APP2")
        self.assertDictEqual(changes3, record5)
        record6 = df.get_record(
            {"tests.test_classes.SmallBase": {"1": 2}}, app="APP2")
        self.assertDictEqual({"gc": dict()}, record6)

    def test_basic_apply_maintain3(self):
        # prof = cProfile.Profile()
        self.maxDiff = None
        obj_count = 1000
        df = ObjectlessDataframe(maintain_change_record=True)
        df.add_type(tc.SmallIntBase)
        df_c1 = dataframe_client()
        df_c1.add_type(tc.SmallIntBase)
        df_c1.start_recording = True

        df_c2 = dataframe_client()
        df_c2.add_type(tc.SmallIntBase)
        base_objs = [tc.SmallIntBase(str(i), i) for i in xrange(obj_count)]
        df_c1.extend(tc.SmallIntBase, base_objs)
        apply_changes1 = df_c1.get_record()
        df_c1.clear_record()
        dfc1_versions = dict()
        vn0 = 0
        # pylint: disable=E1101
        gpname = tc.SmallIntBase.__rtypes_metadata__.name
        # pylint: enable=E1101
        test_objectless_dataframe.apply_versions(
            gpname, apply_changes1, dfc1_versions, vn0)
        # prof.enable()
        df.apply_changes(apply_changes1, except_app="DFC1")
        # prof.disable()

        dfc2_versions = {gpname: dict()}
        # prof.enable()
        return_changes1 = df.get_record(dfc2_versions, "DFC2")
        # prof.disable()
        df_c2.apply_changes(return_changes1)
        test_objectless_dataframe.get_versions(
            gpname, return_changes1, dfc2_versions)

        self.assertSetEqual(set(base_objs), set(df_c2.get(tc.SmallIntBase)))
        version = vn0
        for step in range(10):
            for obj in df_c1.get(tc.SmallIntBase):
                obj.iprop1 += 1
            apply_changes = df_c1.get_record()
            df_c1.clear_record()
            version += 1
            test_objectless_dataframe.apply_versions(
                gpname, apply_changes, dfc1_versions, version)
            # prof.enable()
            df.apply_changes(apply_changes, except_app="DFC1")
            # prof.disable()

            # prof.enable()
            return_changes = df.get_record(dfc2_versions, "DFC2")
            # prof.disable()
            df_c2.apply_changes(return_changes)
            test_objectless_dataframe.get_versions(
                gpname, return_changes, dfc2_versions)

            self.assertSetEqual(
                set(df_c1.get(tc.SmallIntBase)),
                set(df_c2.get(tc.SmallIntBase)))
            for i in range(obj_count):
                self.assertEqual(
                    1, len(df.state_manager.type_to_obj_dimstate[
                        gpname].obj_to_state[str(i)].changes))
        # prof.print_stats(sort="cumulative")

    def test_basic_apply_maintain4(self):
        # prof = cProfile.Profile()
        obj_count = 1000
        self.maxDiff = None
        df = ObjectlessDataframe(maintain_change_record=True)
        df.add_types([tc.SmallIntBase, tc.SubsetOddInt])
        df_c1 = dataframe_client()
        df_c1.add_type(tc.SmallIntBase)
        df_c1.start_recording = True

        df_c2 = dataframe_client()
        df_c2.add_type(tc.SubsetOddInt)

        base_objs = [tc.SmallIntBase(str(i), i) for i in xrange(obj_count)]
        df_c1.extend(tc.SmallIntBase, base_objs)
        apply_changes1 = df_c1.get_record()
        df_c1.clear_record()
        dfc1_versions = dict()
        vn0 = 0
        # pylint: disable=E1101
        gpname = tc.SmallIntBase.__rtypes_metadata__.name
        subsetname = tc.SubsetOddInt.__rtypes_metadata__.name
        # pylint: enable=E1101
        test_objectless_dataframe.apply_versions(
            gpname, apply_changes1, dfc1_versions, vn0)
        # prof.enable()
        df.apply_changes(apply_changes1, except_app="DFC1")
        # prof.disable()
        dfc2_versions = {subsetname: dict()}
        # prof.enable()
        return_changes1 = df.get_record(dfc2_versions, "DFC2")
        # prof.disable()
        df_c2.apply_changes(return_changes1)

        test_objectless_dataframe.get_versions(
            gpname, return_changes1, dfc2_versions, subsetname)
        self.assertSetEqual(
            set((o.oid, o.iprop1)
                for o in base_objs
                if tc.SubsetOddInt.__predicate__(o.iprop1)),
            set((s.oid, s.iprop1) for s in df_c2.get(tc.SubsetOddInt)))

        version = vn0
        for _ in range(10):
            for obj in df_c1.get(tc.SmallIntBase):
                obj.iprop1 += 1
            # prof.enable()
            apply_changes = df_c1.get_record()
            df_c1.clear_record()
            # prof.disable()
            version += 1
            test_objectless_dataframe.apply_versions(
                gpname, apply_changes, dfc1_versions, version)
            # prof.enable()
            df.apply_changes(apply_changes, except_app="DFC1")
            # prof.disable()

            # prof.enable()
            return_changes = df.get_record(dfc2_versions, "DFC2")
            # prof.disable()
            # prof.enable()
            df_c2.apply_changes(return_changes)
            # prof.disable()
            test_objectless_dataframe.get_versions(
                gpname, return_changes, dfc2_versions, subsetname)
            self.assertSetEqual(
                set((o.oid, o.iprop1)
                    for o in base_objs
                    if tc.SubsetOddInt.__predicate__(o.iprop1)),
                set((s.oid, s.iprop1) for s in df_c2.get(tc.SubsetOddInt)))
            subset_oids = set(int(s.oid) for s in df_c2.get(tc.SubsetOddInt))
            for i in xrange(obj_count):
                self.assertEqual(
                    1 if i in subset_oids else 2,
                    len(df.state_manager.type_to_obj_dimstate[
                        gpname].obj_to_state[str(i)].changes))
        # prof.print_stats(sort="cumulative")

    def test_basic_apply_maintain5(self):
        # prof = cProfile.Profile()
        obj_count = 10
        self.maxDiff = None
        df = ObjectlessDataframe(maintain_change_record=True)
        df.add_types(
            [tc.SmallBase, tc.LargeBase, tc.ProjectedJoinSmallAndLargeBase])
        df_c1 = dataframe_client()
        df_c1.add_types([tc.SmallBase, tc.LargeBase])
        df_c1.start_recording = True

        df_c2 = dataframe_client()
        df_c2.add_type(tc.ProjectedJoinSmallAndLargeBase)

        small_base_objs = [
            tc.SmallBase(str(i), i) for i in xrange(obj_count)]
        large_base_objs = [
            tc.LargeBase(str(i), i if i%2 == 0 else 0)
            for i in xrange(obj_count)]
        df_c1.extend(tc.SmallBase, small_base_objs)
        df_c1.extend(tc.LargeBase, large_base_objs)
        apply_changes1 = df_c1.get_record()
        df_c1.clear_record()
        dfc1_versions = dict()
        vn0 = 0
        # pylint: disable=E1101
        smallgpname = tc.SmallBase.__rtypes_metadata__.name
        largegpname = tc.LargeBase.__rtypes_metadata__.name
        joinname = tc.ProjectedJoinSmallAndLargeBase.__rtypes_metadata__.name
        # pylint: enable=E1101
        test_objectless_dataframe.apply_versions(
            smallgpname, apply_changes1, dfc1_versions, vn0)
        test_objectless_dataframe.apply_versions(
            largegpname, apply_changes1, dfc1_versions, vn0)
        # prof.enable()
        df.apply_changes(apply_changes1, except_app="DFC1")
        # prof.disable()
        dfc2_versions = {joinname: dict()}
        # prof.enable()
        return_changes1 = df.get_record(dfc2_versions, "DFC2")
        # prof.disable()
        df_c2.apply_changes(return_changes1)

        self.assertEqual(
            obj_count / 2, len(df_c2.get(tc.ProjectedJoinSmallAndLargeBase)))
        # self.assertSetEqual(
        #     set((o.oid, o.iprop1)
        #         for o in base_objs
        #         if tc.SubsetOddInt.__predicate__(o.iprop1)),
        #     set((s.oid, s.iprop1) for s in df_c2.get(tc.SubsetOddInt)))

        # version = vn0
        # for _ in range(10):
        #     for obj in df_c1.get(tc.SmallIntBase):
        #         obj.iprop1 += 1
        #     # prof.enable()
        #     apply_changes = df_c1.get_record()
        #     df_c1.clear_record()
        #     # prof.disable()
        #     version += 1
        #     test_objectless_dataframe.apply_versions(
        #         gpname, apply_changes, dfc1_versions, version)
        #     # prof.enable()
        #     df.apply_changes(apply_changes, except_app="DFC1")
        #     # prof.disable()

        #     # prof.enable()
        #     return_changes = df.get_record(dfc2_versions, "DFC2")
        #     # prof.disable()
        #     # prof.enable()
        #     df_c2.apply_changes(return_changes)
        #     # prof.disable()
        #     test_objectless_dataframe.get_versions(
        #         gpname, return_changes, dfc2_versions, subsetname)
        #     self.assertSetEqual(
        #         set((o.oid, o.iprop1)
        #             for o in base_objs
        #             if tc.SubsetOddInt.__predicate__(o.iprop1)),
        #         set((s.oid, s.iprop1) for s in df_c2.get(tc.SubsetOddInt)))
        #     subset_oids = set(int(s.oid) for s in df_c2.get(tc.SubsetOddInt))
        #     for i in xrange(obj_count):
        #         self.assertEqual(
        #             1 if i in subset_oids else 2,
        #             len(df.state_manager.type_to_obj_dimstate[
        #                 gpname].obj_to_state[str(i)].changes))
        # prof.print_stats(sort="cumulative")

    def test_basic_apply_maintain6(self):
        # prof = cProfile.Profile()
        obj_count = 10
        self.maxDiff = None
        df = ObjectlessDataframe(maintain_change_record=True)
        df.add_types(
            [tc.SmallBase, tc.ProjectedJoinSmallAndSmallBase])
        df_c1 = dataframe_client()
        df_c1.add_types([tc.SmallBase])
        df_c1.start_recording = True

        df_c2 = dataframe_client()
        df_c2.add_type(tc.ProjectedJoinSmallAndSmallBase)

        base_objs = [
            tc.SmallBase(str(i), i) for i in xrange(obj_count)]
        df_c1.extend(tc.SmallBase, base_objs)
        apply_changes1 = df_c1.get_record()
        df_c1.clear_record()
        dfc1_versions = dict()
        vn0 = 0
        # pylint: disable=E1101
        smallgpname = tc.SmallBase.__rtypes_metadata__.name
        joinname = tc.ProjectedJoinSmallAndSmallBase.__rtypes_metadata__.name
        # pylint: enable=E1101
        test_objectless_dataframe.apply_versions(
            smallgpname, apply_changes1, dfc1_versions, vn0)
        # prof.enable()
        df.apply_changes(apply_changes1, except_app="DFC1")
        # prof.disable()
        dfc2_versions = {joinname: dict()}
        # prof.enable()
        return_changes1 = df.get_record(dfc2_versions, "DFC2")
        # prof.disable()
        df_c2.apply_changes(return_changes1)

        self.assertEqual(
            obj_count, len(df_c2.get(tc.ProjectedJoinSmallAndSmallBase)))
        join_objs = sorted(
            df_c2.get(tc.ProjectedJoinSmallAndSmallBase),
            key=lambda x: x.SB1.oid)
        for i in range(len(join_objs)):
            obj = join_objs[i]
            self.assertEqual(obj.SB1.oid, str(i))
            self.assertEqual(obj.SB1.sprop1, i)

        # self.assertSetEqual(
        #     set((o.oid, o.iprop1)
        #         for o in base_objs
        #         if tc.SubsetOddInt.__predicate__(o.iprop1)),
        #     set((s.oid, s.iprop1) for s in df_c2.get(tc.SubsetOddInt)))

        # version = vn0
        # for _ in range(10):
        #     for obj in df_c1.get(tc.SmallIntBase):
        #         obj.iprop1 += 1
        #     # prof.enable()
        #     apply_changes = df_c1.get_record()
        #     df_c1.clear_record()
        #     # prof.disable()
        #     version += 1
        #     test_objectless_dataframe.apply_versions(
        #         gpname, apply_changes, dfc1_versions, version)
        #     # prof.enable()
        #     df.apply_changes(apply_changes, except_app="DFC1")
        #     # prof.disable()

        #     # prof.enable()
        #     return_changes = df.get_record(dfc2_versions, "DFC2")
        #     # prof.disable()
        #     # prof.enable()
        #     df_c2.apply_changes(return_changes)
        #     # prof.disable()
        #     test_objectless_dataframe.get_versions(
        #         gpname, return_changes, dfc2_versions, subsetname)
        #     self.assertSetEqual(
        #         set((o.oid, o.iprop1)
        #             for o in base_objs
        #             if tc.SubsetOddInt.__predicate__(o.iprop1)),
        #         set((s.oid, s.iprop1) for s in df_c2.get(tc.SubsetOddInt)))
        #     subset_oids = set(int(s.oid) for s in df_c2.get(tc.SubsetOddInt))
        #     for i in xrange(obj_count):
        #         self.assertEqual(
        #             1 if i in subset_oids else 2,
        #             len(df.state_manager.type_to_obj_dimstate[
        #                 gpname].obj_to_state[str(i)].changes))
        # prof.print_stats(sort="cumulative")
