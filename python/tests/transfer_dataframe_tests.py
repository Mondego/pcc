from __future__ import absolute_import

from pcc.attributes import dimension, primarykey
from pcc.set import pcc_set
from pcc.subset import subset
from pcc.impure import impure
from pcc.dataframe import dataframe, DataframeModes
from pcc.parameter import parameter, ParameterMode
from pcc.join import join
from pcc.projection import projection
from pcc.dataframe_changes_pb2 import DataframeChanges, Record, Event, Value
 
import unittest, json

def create_cars():
    @pcc_set
    class Car(object):
        @primarykey(str)
        def oid(self): return self._id

        @oid.setter
        def oid(self, value): self._id = value

        @dimension(int)
        def velocity(self): return self._velocity

        @velocity.setter
        def velocity(self, value): self._velocity = value

        @dimension(str)
        def color(self): return self._color

        @color.setter
        def color(self, value): self._color = value

        def __init__(self, oid, vel, col):
            self.oid = oid
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

def create_complex_cartypes():
    class Vector3(object):
        def __init__(self, X=0.0, Y=0.0, Z=0.0):
            self.X = X
            self.Y = Y
            self.Z = Z

        # -----------------------------------------------------------------
        def VectorDistanceSquared(self, other) :
            dx = self.X - other.X
            dy = self.Y - other.Y
            dz = self.Z - other.Z
            return dx * dx + dy * dy + dz * dz

        # -----------------------------------------------------------------
        def VectorDistance(self, other) :
            return math.sqrt(self.VectorDistanceSquared(other))

        # -----------------------------------------------------------------
        def Length(self) :
            return math.sqrt(self.VectorDistanceSquared(ZeroVector))

        # -----------------------------------------------------------------
        def LengthSquared(self) :
            return self.VectorDistanceSquared(ZeroVector)

        def AddVector(self, other) :
            return Vector3(self.X + other.X, self.Y + other.Y, self.Z + other.Z)

        # -----------------------------------------------------------------
        def SubVector(self, other) :
            return Vector3(self.X - other.X, self.Y - other.Y, self.Z - other.Z)

        # -----------------------------------------------------------------
        def ScaleConstant(self, factor) :
            return Vector3(self.X * factor, self.Y * factor, self.Z * factor)

        # -----------------------------------------------------------------
        def ScaleVector(self, scale) :
            return Vector3(self.X * scale.X, self.Y * scale.Y, self.Z * scale.Z)

        def ToList(self):
            return [self.X, self.Y, self.Z]

        def Rotate(self, rad):
            heading = math.atan(self.Y/self.X)
            return Vector3()

        # -----------------------------------------------------------------
        def Equals(self, other) :
            if isinstance(other, Vector3):
                return self.X == other.X and self.Y == other.Y and self.Z == other.Z
            elif isinstance(other, tuple) or isinstance(other, list):
                return (other[0] == self.X and other[1] == self.Y and other[2] == self.Z)

        # -----------------------------------------------------------------
        def ApproxEquals(self, other, tolerance) :
            return self.VectorDistanceSquared(other) < (tolerance * tolerance)

        def __json__(self):
            return self.__dict__

        def __str__(self):
            return self.__dict__.__str__()

        def __eq__(self, other):
            return self.Equals(other)

        def __ne__(self, other):
            return not self.__eq__(other)

        # -----------------------------------------------------------------
        def __add__(self, other) :
            return self.AddVector(other)

        # -----------------------------------------------------------------
        def __sub__(self, other) :
            return self.SubVector(other)

        # -----------------------------------------------------------------
        def __mul__(self, factor) :
            return self.ScaleConstant(factor)

        # -----------------------------------------------------------------
        def __div__(self, factor) :
            return self.ScaleConstant(1.0 / factor)

        @staticmethod
        def __decode__(dic):
            return Vector3(dic['X'], dic['Y'], dic['Z'])

    ZeroVector = Vector3()

    class Colors:
        Red = 0
        Green = 1
        Blue = 2
        Yellow = 3
        Black = 4
        White = 5
        Grey = 6

    @pcc_set
    class Car(object):
        '''
        classdocs
        '''
        FINAL_POSITION = 700;
        SPEED = 40;

        @primarykey(str)
        def ID(self):
            return self._ID

        @ID.setter
        def ID(self, value):
            self._ID = value

        _Position = Vector3(0, 0, 0)
        @dimension(Vector3)
        def Position(self):
            return self._Position

        @Position.setter
        def Position(self, value):
            self._Position = value

        _Velocity = Vector3(0, 0, 0)
        @dimension(Vector3)
        def Velocity(self):
            return self._Velocity

        @Velocity.setter
        def Velocity(self, value):
            self._Velocity = value

        _Color = Colors.White
        @dimension(Colors)
        def Color(self):
            return self._Color

        @Color.setter
        def Color(self, value):
            self._Color = value

        @dimension(int)
        def Length(self):
            return self._Length

        @Length.setter
        def Length(self, value):
            self._Length = value

        @dimension(int)
        def Width(self):
            return self._Width

        @Width.setter
        def Width(self, value):
            self._Width = value

    @subset(Car)
    class InactiveCar(Car.Class()):
        @staticmethod
        def __query__(cars):
            return [c for c in cars if InactiveCar.__predicate__(c)]

        @staticmethod
        def __predicate__(c):
            return c.Position == Vector3(0,0,0)

        def start(self):
            logger.debug("[InactiveCar]: {0} starting".format(self.ID))
            self.Velocity = Vector3(self.SPEED, 0, 0)

    @subset(Car)
    class ActiveCar(Car.Class()):
        @staticmethod
        def __query__(cars):  # @DontTrace
            return [c for c in cars if ActiveCar.__predicate__(c)]

        @staticmethod
        def __predicate__(c):
            return c.Velocity != Vector3(0,0,0)

        def move(self):
            self.Position = Vector3(self.Position.X + self.Velocity.X, self.Position.Y + self.Velocity.Y, self.Position.Z + self.Velocity.Z)
            logger.debug("[ActiveCar]: Current velocity: {0}, New position {1}".format(self.Velocity, self.Position));

            # End of ride
            if (self.Position.X >= self.FINAL_POSITION or self.Position.Y >= self.FINAL_POSITION):
                self.stop();

        def stop(self):
            logger.debug("[ActiveCar]: {0} stopping".format(self.ID));
            self.Position.X = 0;
            self.Position.Y = 0;
            self.Position.Z = 0;
            self.Velocity.X = 0;
            self.Velocity.Y = 0;
            self.Velocity.Z = 0;

    return Car, InactiveCar, ActiveCar

def create_cars_withobjects():
    class vector(object):
        def __init__(self, x, y, z):
            self.x, self.y, self.z = x, y, z

    @pcc_set
    class Car(object):
        @primarykey(str)
        def oid(self): return self._id

        @oid.setter
        def oid(self, value): self._id = value

        @dimension(vector)
        def velocity(self): return self._velocity

        @velocity.setter
        def velocity(self, value): self._velocity = value

        @dimension(list)
        def color(self): return self._color

        @color.setter
        def color(self, value): self._color = value

        def __init__(self, oid, vel, col):
            self.oid = oid
            self.velocity = vel
            self.color = col

    return Car

def CreateProjectionTypesAndObjects():
    @pcc_set
    class Car(object):
        @primarykey(int)
        def oid(self): return self._id
        @oid.setter
        def oid(self, value): self._id = value
        @dimension(int)
        def owner(self): return self._owner
        @owner.setter
        def owner(self, value): self._owner = value
        @dimension(int)
        def location(self): return self._location
        @location.setter
        def location(self, value): self._location = value
        @dimension(int)
        def velocity(self): return self._velocity
        @velocity.setter
        def velocity(self, value): self._velocity = value

        def __init__(self, id, owner, location, velocity):
            (self.id, self.owner, self.location, self.velocity) = (id, owner, location, velocity)

        def change_owner(self, owner, license):
            self.owner, self.license = owner, license

        def details(self):
            return (self.id, self.owner, self.location, self.velocity)

    @projection(Car, Car.oid, Car.location, Car.velocity)
    class CarForPedestrian(object):
        pass

    car1 = Car(1, "Murugan", "himalaya", 299792458)
    car2 = Car(2, "Shiva", "himalaya", 299792459)

    return Car, CarForPedestrian, [car1, car2] 



def makerecord(dimchange):
    class DimensionType(object):
        Literal = 0
        Object = 1
        ForeignKey = 2
        Collection = 3
        Dictionary = 4
        Unknown = 5

    r = Record()
    if dimchange["type"] == DimensionType.Literal:
        # Have to do weird conversion here :(
        if dimchange["value"] == None:
            r.record_type = Record.NULL
            return r 
        if bool in set(dimchange["value"].__class__.mro()):
            r.value.bool_value = bool(dimchange["value"])
            r.record_type = Record.BOOL
            return r
        if float in set(dimchange["value"].__class__.mro()):
            r.value.float_value = float(dimchange["value"])
            r.record_type = Record.FLOAT
            return r
        if str in set(dimchange["value"].__class__.mro()) or unicode in set(dimchange["value"].__class__.mro()):
            r.value.str_value = str(dimchange["value"])
            r.record_type = Record.STRING
            return r
        if int in set(dimchange["value"].__class__.mro()) or long in set(dimchange["value"].__class__.mro()):
            r.value.int_value = long(dimchange["value"])
            r.record_type = Record.INT
            return r
    if dimchange["type"] == DimensionType.Collection:
        r.value.collection.extend([makerecord(dm) for dm in dimchange["value"]])
        r.record_type = Record.COLLECTION
        return r
    if dimchange["type"] == DimensionType.Dictionary:
        vs = []
        for k, v in dimchange["value"].items():
            one_v = Value.Pair()
            key_r = Record()
            key_r.record_type = Record.STRING
            key_r.value.str_value = k
            one_v.key.CopyFrom(key_r)
            one_v.value.CopyFrom(makerecord(v))
            vs.append(one_v)
        r.value.map.extend(vs)
        r.record_type = Record.DICTIONARY
        return r
    if dimchange["type"] == DimensionType.Object:
        r.value.object.object_map.extend(makerecord({"type": DimensionType.Dictionary, "value": dimchange["value"]}).value.map)
        r.record_type = Record.OBJECT
        return r
    
def convert_json_to_proto(update_json):
    old_record_map = {}
    dfc = DataframeChanges()
    for groupname, groupchanges in update_json.items():
        gc = dfc.group_changes.add()
        gc.group_key = groupname
        for oid, objectchanges in groupchanges.items():
            oc = gc.object_changes.add()
            oc.object_key = oid
            if "dims" in objectchanges:
                for dim, dimchange in objectchanges["dims"].items():
                    dc = oc.dimension_changes.add()
                    dc.dimension_name = dim
                    dc.value.CopyFrom(makerecord(dimchange))
            for tp, status in objectchanges["types"].items():
                tpc = oc.type_changes.add()
                tpc.type.name = tp
                tpc.event = status
    return dfc

update_json1 = convert_json_to_proto({
            "Car": {
                "id1": {
                    "types": {
                        "Car": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id1"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 0
                        },
                        "color": {
                            "type": 0,
                            "value": "BLUE"
                        }
                    }
                },
                "id2": {
                    "types": {
                        "Car": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id2"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 0
                        },
                        "color": {
                            "type": 0,
                            "value": "RED"
                        }
                    }
                },
                "id3": {
                    "types": {
                        "Car": 1,
                        "ActiveCar": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id3"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 1
                        },
                        "color": {
                            "type": 0,
                            "value": "GREEN"
                        }
                    }
                },
                "id4": {
                    "types": {
                        "Car": 1,
                        "ActiveCar": 1,
                        "RedActiveCar": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id4"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 2
                        },
                        "color": {
                            "type": 0,
                            "value": "RED"
                        },
                    },
                }
            }    
        })
resp_json1 = convert_json_to_proto({
            "Car": {
                "id3": {
                    "types": {
                        "ActiveCar": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id3"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 1
                        },
                        "color": {
                            "type": 0,
                            "value": "GREEN"
                        }
                    }
                },
                "id4": {
                    "types": {
                        "ActiveCar": 1,
                        "RedActiveCar": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id4"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 2
                        },
                        "color": {
                            "type": 0,
                            "value": "RED"
                        },
                    },
                }
            }    
        })
update_json2 = convert_json_to_proto({
            "Car": {
                "id2": {
                    "types": {
                        "Car": 2
                    },
                    "dims": {
                        "color": {
                            "type": 0,
                            "value": "GREEN"
                        }
                    }
                }
            }    
        })
update_json3 = convert_json_to_proto({
            "Car": {
                "id2": {
                    "types": {
                        "Car": 2,
                        "ActiveCar": 1,
                        "RedActiveCar": 1
                    },
                    "dims": {
                        "velocity": {
                            "type": 0,
                            "value": "1"
                        }
                    }
                }
            }    
        })
update_json4 = convert_json_to_proto({
            "Car": {
                "id2": {
                    "types": {
                        "ActiveCar": 1,
                        "RedActiveCar": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id2"
                        },
                        "velocity": {
                            "type": 0,
                            "value": "1"
                        },
                        "color": {
                            "type": 0,
                            "value": "RED"
                        }
                    }
                }
            }    
        })
update_json5 = convert_json_to_proto({
            "Car": {
                "id4": {
                    "types": {
                        "Car": 0,
                        "ActiveCar": 0,
                        "RedActiveCar": 0
                    }
                }
            }    
        })
update_json6 = convert_json_to_proto({
            "Car": {
                "id4": {
                    "types": {
                        "Car": 2,
                        "ActiveCar": 0,
                        "RedActiveCar": 0
                    },
                    "dims": {
                        "velocity": {
                            "type": 0,
                            "value": 0
                        }
                    }
                }
            }    
        })
update_json7 = convert_json_to_proto({
            "Car": {
                "id1": {
                    "types": {
                        "Car": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id1"
                        },
                        "velocity": {
                            "type": 1,
                            "value": {
                                "x": { "type": 0, "value": 10 },
                                "y": { "type": 0, "value": 10 },
                                "z": { "type": 0, "value": 10 },
                            }
                        },
                        "color": {
                            "type": 3,
                            "value": [{"type": 0, "value": "BLUE"}]
                        }
                    }
                }
            }    
        })
update_json8 = convert_json_to_proto({
            "Car": {
                "id1": {
                    "types": {
                        "Car": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id1"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 0
                        },
                        "color": {
                            "type": 0,
                            "value": "BLUE"
                        }
                    }
                },
                "id2": {
                    "types": {
                        "Car": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id2"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 0
                        },
                        "color": {
                            "type": 0,
                            "value": "RED"
                        }
                    }
                },
                "id3": {
                    "types": {
                        "Car": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id3"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 1
                        },
                        "color": {
                            "type": 0,
                            "value": "GREEN"
                        }
                    }
                },
                "id4": {
                    "types": {
                        "Car": 1
                    },
                    "dims": {
                        "oid": {
                            "type": 0,
                            "value": "id4"
                        },
                        "velocity": {
                            "type": 0,
                            "value": 2
                        },
                        "color": {
                            "type": 0,
                            "value": "RED"
                        },
                    },
                }
            }    
        })
update_json9 = convert_json_to_proto(json.loads('''{
            "Car": {
                "0fb70042-cb18-4d4c-8ec0-ddfe609f852a": {
                    "dims": {
                        "Color": {
                            "type": 0,
                            "value": 5
                        },
                        "ID": {
                            "type": 0,
                            "value": "0fb70042-cb18-4d4c-8ec0-ddfe609f852a"
                        },
                        "Length": {
                            "type": 0,
                            "value": null
                        },
                        "Position": {
                            "type": 1,
                            "value": {
                                    "X": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": 0,
                                        "value": 0
                                    }
                                }
                        },
                        "Velocity": {
                            "type": 1,
                            "value": {
                                    "X": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": 0,
                                        "value": 0
                                    }
                            }
                        },
                        "Width": {
                            "type": 0,
                            "value": null
                        }
                    },
                    "types": {
                        "Car": 1
                    }
                },
                "7ff34b19-7f30-4c5d-a246-f1668d3b89b9": {
                    "dims": {
                        "Color": {
                            "type": 0,
                            "value": 5
                        },
                        "ID": {
                            "type": 0,
                            "value": "7ff34b19-7f30-4c5d-a246-f1668d3b89b9"
                        },
                        "Length": {
                            "type": 0,
                            "value": null
                        },
                        "Position": {
                            "type": 1,
                            "value": {
                                    "X": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": 0,
                                        "value": 0
                                    }
                                }
                        },
                        "Velocity": {
                            "type": 1,
                            "value": {
                                    "X": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": 0,
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": 0,
                                        "value": 0
                                    }
                            }
                        },
                        "Width": {
                            "type": 0,
                            "value": null
                        }
                    },
                    "types": {
                        "Car": 1
                    }
                }
            }
        }'''))
update_json10 = convert_json_to_proto({
    "Car": {
        "586d5e49-2da7-4318-a09b-795744be9867": {
            "dims": {
                "location": {
                    "type": 0,
                    "value": "himalaya"
                },
                "oid": {
                    "type": 0,
                    "value": "586d5e49-2da7-4318-a09b-795744be9867"
                },
                "owner": {
                    "type": 0,
                    "value": "Shiva"
                },
                "velocity": {
                    "type": 0,
                    "value": 299792459
                }
            },
            "types": {
                "Car": 1
            }
        },
        "8b2658fb-ab86-4e00-96bc-31952d8eb38f": {
            "dims": {
                "location": {
                    "type": 0,
                    "value": "himalaya"
                },
                "oid": {
                    "type": 0,
                    "value": "8b2658fb-ab86-4e00-96bc-31952d8eb38f"
                },
                "owner": {
                    "type": 0,
                    "value": "Murugan"
                },
                "velocity": {
                    "type": 0,
                    "value": 299792458
                }
            },
            "types": {
                "Car": 1
            }
        }
    }
})
                      
resp_json10 = convert_json_to_proto({
    "Car": {
        "586d5e49-2da7-4318-a09b-795744be9867": {
            "dims": {
                "location": {
                    "type": 0,
                    "value": "himalaya"
                },
                "oid": {
                    "type": 0,
                    "value": "586d5e49-2da7-4318-a09b-795744be9867"
                },
                "velocity": {
                    "type": 0,
                    "value": 299792459
                }
            },
            "types": {
                "CarForPedestrian": 1
            }
        },
        "8b2658fb-ab86-4e00-96bc-31952d8eb38f": {
            "dims": {
                "location": {
                    "type": 0,
                    "value": "himalaya"
                },
                "oid": {
                    "type": 0,
                    "value": "8b2658fb-ab86-4e00-96bc-31952d8eb38f"
                },
                "velocity": {
                    "type": 0,
                    "value": 299792458
                }
            },
            "types": {
                "CarForPedestrian": 1
            }
        }
    }
})

class Test_dataframe_transfer_tests(unittest.TestCase):
    def test_dataframe_apply_all_new(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.apply_all(update_json1)
        self.assertTrue(len(df.get(Car)) == 4)
        self.assertTrue(len(df.get(ActiveCar)) == 2)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)

    def test_dataframe_apply_mod1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        
        df.apply_all(update_json2)
        self.assertTrue(len(df.get(Car)) == 5)
        self.assertTrue(df.object_map["Car"]["id2"].color == "GREEN")  

    def test_dataframe_apply_mod2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        self.assertTrue(len(df.get(Car)) == 5)
        self.assertTrue(len(df.get(ActiveCar)) == 3)
        self.assertTrue(len(df.get(RedActiveCar)) == 2)
        df.apply_all(update_json3)
        self.assertTrue(len(df.get(Car)) == 5)
        self.assertTrue(len(df.get(ActiveCar)) == 4)
        self.assertTrue(len(df.get(RedActiveCar)) == 3)
    
    def test_dataframe_apply_no_base(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([ActiveCar, RedActiveCar])
        df.apply_all(update_json4)
        self.assertTrue(len(df.get(ActiveCar)) == 1)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)
        rac = df.get(RedActiveCar)[0]
        rac.velocity = 5
        ac = df.get(ActiveCar)[0]
        self.assertTrue(ac.velocity == 5)
        try:
            df.get(Car)
            self.fail()
        except TypeError:
            self.assertTrue(True)
        
    def test_dataframe_apply_delete1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)

        self.assertTrue(len(df.get(ActiveCar)) == 3)
        self.assertTrue(len(df.get(RedActiveCar)) == 2)
        self.assertTrue(len(df.get(Car)) == 5)
        
        df.apply_all(update_json5)
        self.assertTrue(len(df.get(ActiveCar)) == 2)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)
        self.assertTrue(len(df.get(Car)) == 4)
                         
    def test_dataframe_apply_delete2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)

        self.assertTrue(len(df.get(ActiveCar)) == 3)
        self.assertTrue(len(df.get(RedActiveCar)) == 2)
        self.assertTrue(len(df.get(Car)) == 5)
        
        df.apply_all(update_json6)
        self.assertTrue(len(df.get(ActiveCar)) == 2)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)
        self.assertTrue(len(df.get(Car)) == 5)
                         
    def test_dataframe_apply_with_normal_objects(self):
        Car = create_cars_withobjects()
        df = dataframe()
        df.add_types([Car])
        df.apply_all(update_json7)
        self.assertTrue(len(df.get(Car)) == 1)
        c = df.get(Car)[0]
        self.assertTrue(c.velocity.x == 10 and c.velocity.y == 10 and c.velocity.z == 10)
        self.assertTrue(c.color == ["BLUE"])

    def test_dataframe_get_changes1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        df.start_recording = True
        for c in df.get(RedActiveCar):
            c.velocity = 0
        #print json.dumps(df.get_record(), sort_keys = True, separators = (',', ': '), indent = 4) 
        self.assertTrue(df.get_record() == convert_json_to_proto({
            "Car" :{
                "id4" :{
                    "dims" :{
                        "velocity" :{
                            "type" :0,
                            "value" :0
                        }
                    },
                    "types" :{
                        "ActiveCar" :0,
                        "Car" :2,
                        "RedActiveCar" :0
                    }
                },
                "id5" :{
                    "dims" :{
                        "velocity" :{
                            "type" :0,
                            "value" :0
                        }
                    },
                    "types" :{
                        "ActiveCar" :0,
                        "Car" :2,
                        "RedActiveCar" :0
                    }
                }
            }
        }))

    def test_dataframe_get_changes2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        df.start_recording = True
        for c in df.get(Car):
            c.velocity += 1
        #print json.dumps(df.get_record(), sort_keys = True, separators = (',', ': '), indent = 4) 
        self.assertTrue(df.get_record() == convert_json_to_proto({
            "Car" :{
                "id1" :{
                    "dims" :{
                        "color" :{
                            "type" :0,
                            "value" :"BLUE"
                        },
                        "oid" :{
                            "type" :0,
                            "value" :"id1"
                        },
                        "velocity" :{
                            "type" :0,
                            "value" :1
                        }
                    },
                    "types" :{
                        "ActiveCar" :1,
                        "Car" :2
                    }
                },
                "id2" :{
                    "dims" :{
                        "color" :{
                            "type" :0,
                            "value" :"RED"
                        },
                        "oid" :{
                            "type" :0,
                            "value" :"id2"
                        },
                        "velocity" :{
                            "type" :0,
                            "value" :1
                        }
                    },
                    "types" :{
                        "ActiveCar" :1,
                        "Car" :2,
                        "RedActiveCar" :1
                    }
                },
                "id3" :{
                    "dims" :{
                        "velocity" :{
                            "type" :0,
                            "value" :2
                        }
                    },
                    "types" :{
                        "ActiveCar" :2,
                        "Car" :2
                    }
                },
                "id4" :{
                    "dims" :{
                        "velocity" :{
                            "type" :0,
                            "value" :2
                        }
                    },
                    "types" :{
                        "ActiveCar" :2,
                        "Car" :2,
                        "RedActiveCar" :2
                    }
                },
                "id5" :{
                    "dims" :{
                        "velocity" :{
                            "type" :0,
                            "value" :3
                        }
                    },
                    "types" :{
                        "ActiveCar" :2,
                        "Car" :2,
                        "RedActiveCar" :2
                    }
                }
            }
        }))

    def test_dataframe_transfer_changes3(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df1 = dataframe()
        df1.add_types([Car, ActiveCar, RedActiveCar])
        df1.extend(Car, cars)
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df2 = dataframe()
        df2.add_types([Car, ActiveCar, RedActiveCar])
        df2.extend(Car, cars) # So they are not linked by reference.
        #print len(df1.get(ActiveCar)), len(df2.get(ActiveCar))
        
        df1.start_recording = True
        for c in df1.get(Car):
            c.velocity += 1
        self.assertFalse(len(df1.get(ActiveCar)) == len(df2.get(ActiveCar)))
        #print len(df1.get(ActiveCar)), len(df2.get(ActiveCar))
        df2.apply_all(df1.get_record())
        #print len(df1.get(ActiveCar)), len(df2.get(ActiveCar))
        self.assertTrue(len(df1.get(ActiveCar)) == len(df2.get(ActiveCar)))

    def test_dataframe_transfer_changes4(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df1 = dataframe()
        df1.add_types([Car, ActiveCar, RedActiveCar])
        df1.extend(Car, cars)
        df2 = dataframe()
        df2.add_types([Car, ActiveCar, RedActiveCar])
        df2.extend(Car, cars) # Linked by reference
        
        df1.start_recording = True
        df2.start_recording = True
        for c in df1.get(Car):
            c.velocity += 1
        self.assertTrue(df1.get_record() == df2.get_record())

    def test_dataframe_apply_get_new(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        df.apply_all(update_json1)

        self.assertTrue(len(df.get_new(Car)) == 4)
        self.assertTrue(len(df.get_mod(Car)) == 0)  
        self.assertTrue(len(df.get_deleted(Car)) == 0)  
        self.assertTrue(len(df.get_new(ActiveCar)) == 2)
        self.assertTrue(len(df.get_mod(ActiveCar)) == 0)
        self.assertTrue(len(df.get_deleted(ActiveCar)) == 0)  
        self.assertTrue(len(df.get_new(RedActiveCar)) == 1)
        self.assertTrue(len(df.get_mod(RedActiveCar)) == 0)
        self.assertTrue(len(df.get_deleted(RedActiveCar)) == 0)  

    def test_dataframe_apply_get_mod(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        df.apply_all(update_json2)
        self.assertTrue(len(df.get_new(Car)) == 0)
        self.assertTrue(len(df.get_mod(Car)) == 1)  
        self.assertTrue(len(df.get_deleted(Car)) == 0)  
        self.assertTrue(len(df.get_new(ActiveCar)) == 0)
        self.assertTrue(len(df.get_new(RedActiveCar)) == 0)
        self.assertTrue(len(df.get_mod(RedActiveCar)) == 0)
        self.assertTrue(len(df.get_deleted(RedActiveCar)) == 0)  

    def test_dataframe_apply_get_mod2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        df.apply_all(update_json3)
        self.assertTrue(len(df.get_new(Car)) == 0)
        self.assertTrue(len(df.get_mod(Car)) == 1)  
        self.assertTrue(len(df.get_deleted(Car)) == 0)  
        self.assertTrue(len(df.get_new(ActiveCar)) == 1)
        self.assertTrue(len(df.get_mod(ActiveCar)) == 0)
        self.assertTrue(len(df.get_deleted(ActiveCar)) == 0)  
        self.assertTrue(len(df.get_new(RedActiveCar)) == 1)
        self.assertTrue(len(df.get_mod(RedActiveCar)) == 0)
        self.assertTrue(len(df.get_deleted(RedActiveCar)) == 0)  

    def test_dataframe_apply_get_delete1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        df.apply_all(update_json6)

        self.assertTrue(len(df.get_new(Car)) == 0)
        self.assertTrue(len(df.get_mod(Car)) == 1)  
        self.assertTrue(len(df.get_deleted(Car)) == 0)  
        self.assertTrue(len(df.get_new(ActiveCar)) == 0)
        self.assertTrue(len(df.get_mod(ActiveCar)) == 0)
        self.assertTrue(len(df.get_deleted(ActiveCar)) == 1)  
        self.assertTrue(len(df.get_new(RedActiveCar)) == 0)
        self.assertTrue(len(df.get_mod(RedActiveCar)) == 0)
        self.assertTrue(len(df.get_deleted(RedActiveCar)) == 1)  

    def test_dataframe_apply_between_master_slaves1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df_m = dataframe()
        df_s = dataframe(mode = DataframeModes.ApplicationCache)
        df_m.add_types([Car, ActiveCar, RedActiveCar])
        df_s.add_types([Car, ActiveCar, RedActiveCar])
        df_m.connect(df_s)
        df_s.start_recording = True
        df_m.apply_all(update_json1)
        #print json.dumps(df_s.get_record(), sort_keys = True, separators = (',', ': '), indent = 4) 
        self.assertTrue(df_s.get_record() == update_json1)
        self.assertTrue(df_s.get(Car) == df_m.get(Car))
        self.assertTrue(df_s.get(ActiveCar) == df_m.get(ActiveCar))
        self.assertTrue(df_s.get(RedActiveCar) == df_m.get(RedActiveCar))

    def test_dataframe_apply_between_master_slaves2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df_m = dataframe()
        df_s = dataframe(mode = DataframeModes.ApplicationCache)
        df_m.add_types([Car, ActiveCar, RedActiveCar])
        df_s.add_types([ActiveCar, RedActiveCar])
        df_m.connect(df_s)
        df_s.start_recording = True
        df_m.apply_all(update_json1)
        #print json.dumps(df_s.get_record(), sort_keys = True, separators = (',', ': '), indent = 4) 
        self.assertTrue(df_s.get_record() == resp_json1)
        try:
            df_s.get(Car)
            self.fail("Didn't catch exception at previous line")
        except TypeError:
            self.assertTrue(True)
        self.assertTrue(len(df_m.get(Car)) == 4)
        self.assertTrue(df_s.get(ActiveCar) == df_m.get(ActiveCar))
        self.assertTrue(df_s.get(RedActiveCar) == df_m.get(RedActiveCar))
    
    def test_dataframe_apply_between_master_slaves3(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df_m = dataframe()
        df_s = dataframe(mode = DataframeModes.ApplicationCache)
        df_m.add_types([Car, ActiveCar, RedActiveCar])
        df_s.add_types([ActiveCar, RedActiveCar])
        df_m.connect(df_s)
        df_s.start_recording = True
        df_m.apply_all(update_json8)
        #print json.dumps(df_s.get_record(), sort_keys = True, separators = (',', ': '), indent = 4) 
        self.assertTrue(df_s.get_record() == resp_json1)
        try:
            df_s.get(Car)
            self.fail("Didn't catch exception at previous line")
        except TypeError:
            self.assertTrue(True)
        self.assertTrue(len(df_m.get(Car)) == 4)
        self.assertTrue(df_s.get(ActiveCar) == df_m.get(ActiveCar))
        self.assertTrue(df_s.get(RedActiveCar) == df_m.get(RedActiveCar))
        
    def test_dataframe_apply_to_client1(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe(mode = DataframeModes.Client)
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.apply_all(update_json8)
        self.assertTrue(len(df.get(Car)) == 4)
        self.assertTrue(len(df.get(ActiveCar)) == 0)
        self.assertTrue(len(df.get(RedActiveCar)) == 0)

    def test_dataframe_apply_to_client2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe(mode = DataframeModes.Client)
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.apply_all(update_json1)
        self.assertTrue(len(df.get(Car)) == 4)
        self.assertTrue(len(df.get(ActiveCar)) == 2)
        self.assertTrue(len(df.get(RedActiveCar)) == 1)

    def test_dataframe_apply_all_new_full_objs(self):
        Car, InactiveCar, ActiveCar = create_complex_cartypes()
        df = dataframe()
        df.add_types([Car, ActiveCar, InactiveCar])
        df.apply_all(update_json9)
        self.assertTrue(len(df.get(Car)) == 2)
        self.assertTrue(len(df.get(InactiveCar)) == 2)
        self.assertTrue(len(df.get(ActiveCar)) == 0)

    def test_dataframe_apply_projections(self):
        Car, CarForPedestrian, cars = CreateProjectionTypesAndObjects()
        df = dataframe()
        df.add_types([Car, CarForPedestrian])
        df.apply_all(update_json10)
        self.assertTrue(len(df.get(Car)) == 2)
        self.assertTrue(len(df.get(CarForPedestrian)) == 2)
        for c in df.get(CarForPedestrian):
            self.assertFalse(hasattr(c, "owner"))
            self.assertTrue(hasattr(c, "location"))
        
    def test_dataframe_apply_projections_cache(self):
        Car, CarForPedestrian, cars = CreateProjectionTypesAndObjects()
        df = dataframe()
        df.add_types([Car, CarForPedestrian])
        df_cache = dataframe(mode = DataframeModes.ApplicationCache)
        df_cache.add_type(CarForPedestrian)
        df_cache.start_recording = True
        df.connect(df_cache)
        df.apply_all(update_json10)
        self.assertTrue(len(df.get(Car)) == 2)
        self.assertTrue(len(df.get(CarForPedestrian)) == 2)
        #print json.dumps(df_cache.get_record(), sort_keys = True, separators = (',', ': '), indent = 4) 
        self.assertTrue(df_cache.get_record() == resp_json10)
    
    def test_dataframe_apply_serialize_all(self):
        Car, CarForPedestrian, cars = CreateProjectionTypesAndObjects()
        df = dataframe()
        df.add_types([Car, CarForPedestrian])
        df.extend(Car, cars)
        serialized = df.serialize_all()
        self.assertTrue(len(serialized.group_changes) == 1)
        self.assertTrue("Car" == serialized.group_changes[0].group_key)
        self.assertTrue(len(serialized.group_changes[0].object_changes) == 2)
        for obj_c in serialized.group_changes[0].object_changes:
            oid = obj_c.object_key
            self.assertTrue(len(obj_c.dimension_changes) == 4)
            self.assertTrue(len(obj_c.type_changes) == 2)