from __future__ import absolute_import

from pcc.attributes import dimension, primarykey
from pcc.set import pcc_set
from pcc.subset import subset
from pcc.impure import impure
from pcc.dataframe import dataframe
from pcc.parameter import parameter, ParameterMode
from pcc.join import join
    
import unittest

def create_cars():
    @pcc_set
    class Car(object):
        @primarykey(str)
        def id(self): return self._id

        @id.setter
        def id(self, value): self._id = value

        @dimension(int)
        def velocity(self): return self._velocity

        @velocity.setter
        def velocity(self, value): self._velocity = value

        @dimension(str)
        def color(self): return self._color

        @color.setter
        def color(self, value): self._color = value

        def __init__(self, id, vel, col):
            self.id = id
            self.velocity = vel
            self.color = col


    @subset(Car)
    class ActiveCar(Car.Class()):
        @staticmethod
        def __predicate__(c):
            return c.velocity != 0

    @subset(ActiveCar)
    class RedActiveCar(Car.Class()):
        @staticmethod
        def __predicate__(ac):
            return ac.color == "RED"

    cars = [Car("id1", 0, "BLUE"), 
            Car("id2", 0, "RED"), 
            Car("id3", 1, "GREEN"), 
            Car("id4", 1, "RED"), 
            Car("id5", 2, "RED")]
    return Car, ActiveCar, RedActiveCar, cars

def create_cars_withobjects():
    class vector(object):
        def __init__(self, x, y, z):
            self.x, self.y, self.z = x, y, z

    @pcc_set
    class Car(object):
        @primarykey(str)
        def id(self): return self._id

        @id.setter
        def id(self, value): self._id = value

        @dimension(vector)
        def velocity(self): return self._velocity

        @velocity.setter
        def velocity(self, value): self._velocity = value

        @dimension(list)
        def color(self): return self._color

        @color.setter
        def color(self, value): self._color = value

        def __init__(self, id, vel, col):
            self.id = id
            self.velocity = vel
            self.color = col

    return Car


class Test_dataframe_transfer_tests(unittest.TestCase):
    def test_dataframe_apply_all_new(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        update_json = {
            "Car": {
                "id1": {
                    "types": {
                        "Car": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id1"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": "0"
                        },
                        "color": {
                            "type": "literal",
                            "value": "BLUE"
                        }
                    }
                },
                "id2": {
                    "types": {
                        "Car": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id2"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": "0"
                        },
                        "color": {
                            "type": "literal",
                            "value": "RED"
                        }
                    }
                },
                "id3": {
                    "types": {
                        "Car": "new",
                        "ActiveCar": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id3"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": "1"
                        },
                        "color": {
                            "type": "literal",
                            "value": "GREEN"
                        }
                    }
                },
                "id4": {
                    "types": {
                        "Car": "new",
                        "ActiveCar": "new",
                        "RedActiveCar": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id4"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": "2"
                        },
                        "color": {
                            "type": "literal",
                            "value": "RED"
                        },
                    },
                }
            }    
        }
        df.apply_all(update_json)
        self.assertTrue(len(df.get(Car)) == 4)
        self.assertTrue(len(df.get(ActiveCar)) == 2)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)

    def test_dataframe_apply_mod1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        update_json = {
            "Car": {
                "id2": {
                    "types": {
                        "Car": "mod"
                    },
                    "dims": {
                        "color": {
                            "type": "literal",
                            "value": "GREEN"
                        }
                    }
                }
            }    
        }
        df.apply_all(update_json)
        self.assertTrue(len(df.get(Car)) == 5)
        self.assertTrue(df.object_map["Car"]["id2"].color == "GREEN")  

    def test_dataframe_apply_mod2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        update_json = {
            "Car": {
                "id2": {
                    "types": {
                        "Car": "mod",
                        "ActiveCar": "new",
                        "RedActiveCar": "new"
                    },
                    "dims": {
                        "velocity": {
                            "type": "literal",
                            "value": "1"
                        }
                    }
                }
            }    
        }
        self.assertTrue(len(df.get(Car)) == 5)
        self.assertTrue(len(df.get(ActiveCar)) == 3)
        self.assertTrue(len(df.get(RedActiveCar)) == 2)
        df.apply_all(update_json)
        self.assertTrue(len(df.get(Car)) == 5)
        self.assertTrue(len(df.get(ActiveCar)) == 4)
        self.assertTrue(len(df.get(RedActiveCar)) == 3)
    
    def test_dataframe_apply_no_base(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([ActiveCar, RedActiveCar])
        update_json = {
            "Car": {
                "id2": {
                    "types": {
                        "ActiveCar": "new",
                        "RedActiveCar": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id2"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": "1"
                        },
                        "color": {
                            "type": "literal",
                            "value": "RED"
                        }
                    }
                }
            }    
        }
        df.apply_all(update_json)
        self.assertTrue(len(df.get(ActiveCar)) == 1)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)
        rac = df.get(RedActiveCar)[0]
        rac.velocity = 5
        ac = df.get(ActiveCar)[0]
        self.assertTrue(ac.velocity == 5)
        try:
            df.get(Car)
            self.fail()
        except KeyError:
            self.assertTrue(True)
        
    def test_dataframe_apply_delete1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)

        update_json = {
            "Car": {
                "id4": {
                    "types": {
                        "Car": "delete",
                        "ActiveCar": "delete",
                        "RedActiveCar": "delete"
                    }
                }
            }    
        }
        self.assertTrue(len(df.get(ActiveCar)) == 3)
        self.assertTrue(len(df.get(RedActiveCar)) == 2)
        self.assertTrue(len(df.get(Car)) == 5)
        
        df.apply_all(update_json)
        self.assertTrue(len(df.get(ActiveCar)) == 2)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)
        self.assertTrue(len(df.get(Car)) == 4)
                         
    def test_dataframe_apply_delete2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)

        update_json = {
            "Car": {
                "id4": {
                    "types": {
                        "Car": "mod",
                        "ActiveCar": "delete",
                        "RedActiveCar": "delete"
                    },
                    "dims": {
                        "velocity": {
                            "type": "literal",
                            "value": "0"
                        }
                    }
                }
            }    
        }
        self.assertTrue(len(df.get(ActiveCar)) == 3)
        self.assertTrue(len(df.get(RedActiveCar)) == 2)
        self.assertTrue(len(df.get(Car)) == 5)
        
        df.apply_all(update_json)
        self.assertTrue(len(df.get(ActiveCar)) == 2)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)
        self.assertTrue(len(df.get(Car)) == 5)
                         
    def test_dataframe_apply_with_normal_objects(self):
        Car = create_cars_withobjects()
        df = dataframe()
        df.add_types([Car])
        update_json = {
            "Car": {
                "id1": {
                    "types": {
                        "Car": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id1"
                        },
                        "velocity": {
                            "type": "object",
                            "value": {
                                "x": { "type": "literal", "value": 10 },
                                "y": { "type": "literal", "value": 10 },
                                "z": { "type": "literal", "value": 10 },
                            }
                        },
                        "color": {
                            "type": "list",
                            "value": [{"type": "literal", "value": "BLUE"}]
                        }
                    }
                }
            }    
        }
        df.apply_all(update_json)
        self.assertTrue(len(df.get(Car)) == 1)
        c = df.get(Car)[0]
        self.assertTrue(c.velocity.x == 10 and c.velocity.y == 10 and c.velocity.z == 10)
        self.assertTrue(c.color == ["BLUE"])
