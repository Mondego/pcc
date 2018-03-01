import unittest, json

from tests import test_classes as tc
from rtypes.dataframe.objectless_dataframe import ObjectlessDataframe
from rtypes.dataframe.dataframe_client import dataframe_client
from rtypes.pcc.utils.enums import Record, Event

class test_objectless_dataframe(unittest.TestCase):
    def test_basic_apply(self):
        self.maxDiff = None
        df = ObjectlessDataframe()
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
        record1 = df.get_record({"tests.test_classes.SmallBase": dict()})
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
        record3 = df.get_record({"tests.test_classes.SmallBase": dict()})
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
        
        record5 = df.get_record({"tests.test_classes.SmallBase": dict()})
        new_version = record5["gc"]["tests.test_classes.SmallBase"]["1"]["version"][1]

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
                        "dims": dict(),
                        "types": {
                            "tests.test_classes.SmallBase": Event.Modification
                        },
                        "version": [10, new_version]
                    }
                }
            }
        }, record7)
