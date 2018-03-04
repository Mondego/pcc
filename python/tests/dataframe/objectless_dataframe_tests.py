import unittest, json

from tests import test_classes as tc
from rtypes.dataframe.objectless_dataframe import ObjectlessDataframe
from rtypes.dataframe.dataframe_client import dataframe_client
from rtypes.pcc.utils.enums import Record, Event
import cProfile

class test_objectless_dataframe(unittest.TestCase):
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
        self.maxDiff = None
        df = ObjectlessDataframe(maintain_change_record=True)
        df.add_type(tc.SmallIntBase)
        df_c1 = dataframe_client()
        df_c1.add_type(tc.SmallIntBase)
        df_c1.start_recording = True

        df_c2 = dataframe_client()
        df_c2.add_type(tc.SmallIntBase)
        base_objs = [tc.SmallIntBase(str(i), i) for i in xrange(1000)]
        df_c1.extend(tc.SmallIntBase, base_objs)
        apply_changes1 = df_c1.get_record()
        df_c1.clear_record()
        dfc1_versions = dict()
        vn0 = 0
        gpname = tc.SmallIntBase.__rtypes_metadata__.name
        for oid, obchanges in apply_changes1["gc"][gpname].iteritems():
            dfc1_versions.setdefault(gpname, dict())[oid] = vn0
            obchanges["version"] = [None, vn0]
        df.apply_changes(apply_changes1, except_app="DFC1")

        dfc2_versions = {gpname: dict()}
        return_changes1 = df.get_record(dfc2_versions, "DFC2")
        df_c2.apply_changes(return_changes1)
        for oid, obchanges in return_changes1["gc"][gpname].iteritems():
            dfc2_versions[gpname][oid] = obchanges["version"][1]

        self.assertSetEqual(set(base_objs), set(df_c2.get(tc.SmallIntBase)))

        for obj in df_c1.get(tc.SmallIntBase):
            obj.iprop1 += 1
        apply_changes2 = df_c1.get_record()
        df_c1.clear_record()
        vn1 = 1
        for oid, obchanges in apply_changes2["gc"][gpname].iteritems():
            dfc1_versions.setdefault(gpname, dict())[oid] = vn1
            obchanges["version"] = [vn0, vn1]
        df.apply_changes(apply_changes2, except_app="DFC1")

        return_changes2 = df.get_record(dfc2_versions, "DFC2")
        df_c2.apply_changes(return_changes2)
        for oid, obchanges in return_changes2["gc"][gpname].iteritems():
            dfc2_versions[gpname][oid] = obchanges["version"][1]

        self.assertSetEqual(
            set(df_c1.get(tc.SmallIntBase)), set(df_c2.get(tc.SmallIntBase)))
