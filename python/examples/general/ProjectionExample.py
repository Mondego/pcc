'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from pcc import projection
from pcc import dataframe
from pcc import dimension

class Car(object):
    @dimension(int)
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @dimension(int)
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        self._owner = value

    @dimension(int)
    def location(self):
        return self._location

    @location.setter
    def location(self, value):
        self._location = value

    @dimension(int)
    def velocity(self):
        return self._velocity

    @velocity.setter
    def velocity(self, value):
        self._velocity = value

    def __init__(self, id, owner, location, velocity):
        (self.id,
         self.owner,
         self.location,
         self.velocity) = (id, owner, location, velocity)

    def change_owner(self, owner, license):
        self.owner, self.license = owner, license

    def details(self):
        print (self.id,
         self.owner,
         self.location,
         self.velocity)

@projection(Car, Car.location, Car.velocity)
class CarForPedestrian(object):
    pass

car1 = Car(1,
                     "Murugan",
                     "himalaya",
                     299792458)

with dataframe() as df:
    for car in df.add(CarForPedestrian, [car1]):
        print "location, velocity:", car.location, car.velocity
        try:
            print "owner, license", car.owner, car.license
        except Exception:
            print "Other attributes are not there in projection"
        car.location, car.velocity = "western ghats", 299792459

car1.details()


