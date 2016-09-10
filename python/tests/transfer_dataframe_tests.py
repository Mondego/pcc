from __future__ import absolute_import

from pcc.attributes import dimension, primarykey
from pcc.set import pcc_set
from pcc.subset import subset
from pcc.impure import impure
from pcc.dataframe import dataframe, DataframeModes
from pcc.parameter import parameter, ParameterMode
from pcc.join import join
    
import unittest, json

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

update_json1 = {
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
                            "value": 0
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
                            "value": 0
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
                            "value": 1
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
                            "value": 2
                        },
                        "color": {
                            "type": "literal",
                            "value": "RED"
                        },
                    },
                }
            }    
        }
resp_json1 = {
            "Car": {
                "id3": {
                    "types": {
                        "ActiveCar": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id3"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": 1
                        },
                        "color": {
                            "type": "literal",
                            "value": "GREEN"
                        }
                    }
                },
                "id4": {
                    "types": {
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
                            "value": 2
                        },
                        "color": {
                            "type": "literal",
                            "value": "RED"
                        },
                    },
                }
            }    
        }
update_json2 = {
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
update_json3 = {
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
update_json4 = {
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
update_json5 = {
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
update_json6 = {
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
                            "value": 0
                        }
                    }
                }
            }    
        }
update_json7 = {
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
update_json8 = {
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
                            "value": 0
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
                            "value": 0
                        },
                        "color": {
                            "type": "literal",
                            "value": "RED"
                        }
                    }
                },
                "id3": {
                    "types": {
                        "Car": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id3"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": 1
                        },
                        "color": {
                            "type": "literal",
                            "value": "GREEN"
                        }
                    }
                },
                "id4": {
                    "types": {
                        "Car": "new"
                    },
                    "dims": {
                        "id": {
                            "type": "literal",
                            "value": "id4"
                        },
                        "velocity": {
                            "type": "literal",
                            "value": 2
                        },
                        "color": {
                            "type": "literal",
                            "value": "RED"
                        },
                    },
                }
            }    
        }
update_json9 = '''{
            "Car": {
                "0fb70042-cb18-4d4c-8ec0-ddfe609f852a": {
                    "dims": {
                        "Color": {
                            "type": "literal",
                            "value": 5
                        },
                        "ID": {
                            "type": "literal",
                            "value": "0fb70042-cb18-4d4c-8ec0-ddfe609f852a"
                        },
                        "Length": {
                            "type": "literal",
                            "value": null
                        },
                        "Position": {
                            "type": "object",
                            "value": {
                                    "X": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": "literal",
                                        "value": 0
                                    }
                                }
                        },
                        "Velocity": {
                            "type": "object",
                            "value": {
                                    "X": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": "literal",
                                        "value": 0
                                    }
                            }
                        },
                        "Width": {
                            "type": "literal",
                            "value": null
                        }
                    },
                    "types": {
                        "Car": "new"
                    }
                },
                "7ff34b19-7f30-4c5d-a246-f1668d3b89b9": {
                    "dims": {
                        "Color": {
                            "type": "literal",
                            "value": 5
                        },
                        "ID": {
                            "type": "literal",
                            "value": "7ff34b19-7f30-4c5d-a246-f1668d3b89b9"
                        },
                        "Length": {
                            "type": "literal",
                            "value": null
                        },
                        "Position": {
                            "type": "object",
                            "value": {
                                    "X": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": "literal",
                                        "value": 0
                                    }
                                }
                        },
                        "Velocity": {
                            "type": "object",
                            "value": {
                                    "X": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Y": {
                                        "type": "literal",
                                        "value": 0
                                    },
                                    "Z": {
                                        "type": "literal",
                                        "value": 0
                                    }
                            }
                        },
                        "Width": {
                            "type": "literal",
                            "value": null
                        }
                    },
                    "types": {
                        "Car": "new"
                    }
                }
            }
        }'''
                            

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
        self.assertTrue(df.get_record() == {
            "Car" :{
                "id4" :{
                    "dims" :{
                        "velocity" :{
                            "type" :"literal",
                            "value" :0
                        }
                    },
                    "types" :{
                        "ActiveCar" :"delete",
                        "Car" :"mod",
                        "RedActiveCar" :"delete"
                    }
                },
                "id5" :{
                    "dims" :{
                        "velocity" :{
                            "type" :"literal",
                            "value" :0
                        }
                    },
                    "types" :{
                        "ActiveCar" :"delete",
                        "Car" :"mod",
                        "RedActiveCar" :"delete"
                    }
                }
            }
        })

    def test_dataframe_get_changes2(self):
        Car, ActiveCar, RedActiveCar, cars = create_cars()
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        df.start_recording = True
        for c in df.get(Car):
            c.velocity += 1
        #print json.dumps(df.get_record(), sort_keys = True, separators = (',', ': '), indent = 4) 
        self.assertTrue(df.get_record() == {
            "Car" :{
                "id1" :{
                    "dims" :{
                        "color" :{
                            "type" :"literal",
                            "value" :"BLUE"
                        },
                        "id" :{
                            "type" :"literal",
                            "value" :"id1"
                        },
                        "velocity" :{
                            "type" :"literal",
                            "value" :1
                        }
                    },
                    "types" :{
                        "ActiveCar" :"new",
                        "Car" :"mod"
                    }
                },
                "id2" :{
                    "dims" :{
                        "color" :{
                            "type" :"literal",
                            "value" :"RED"
                        },
                        "id" :{
                            "type" :"literal",
                            "value" :"id2"
                        },
                        "velocity" :{
                            "type" :"literal",
                            "value" :1
                        }
                    },
                    "types" :{
                        "ActiveCar" :"new",
                        "Car" :"mod",
                        "RedActiveCar" :"new"
                    }
                },
                "id3" :{
                    "dims" :{
                        "velocity" :{
                            "type" :"literal",
                            "value" :2
                        }
                    },
                    "types" :{
                        "ActiveCar" :"mod",
                        "Car" :"mod"
                    }
                },
                "id4" :{
                    "dims" :{
                        "velocity" :{
                            "type" :"literal",
                            "value" :2
                        }
                    },
                    "types" :{
                        "ActiveCar" :"mod",
                        "Car" :"mod",
                        "RedActiveCar" :"mod"
                    }
                },
                "id5" :{
                    "dims" :{
                        "velocity" :{
                            "type" :"literal",
                            "value" :3
                        }
                    },
                    "types" :{
                        "ActiveCar" :"mod",
                        "Car" :"mod",
                        "RedActiveCar" :"mod"
                    }
                }
            }
        })

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
        df.apply_all(json.loads(update_json9))
        self.assertTrue(len(df.get(Car)) == 2)
        self.assertTrue(len(df.get(InactiveCar)) == 2)
        self.assertTrue(len(df.get(ActiveCar)) == 0)
        

    