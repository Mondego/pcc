from pcc.projection import projection
from pcc.dataframe import dataframe

class Car(object):
  def __init__(self, id, owner, vin, license, make, model, year, mileage, location, velocity):
    (self.id,
     self.owner,
     self.vin,
     self.license,
     self.make,
     self.model,
     self.year,
     self.mileage,
     self.location,
     self.velocity) = (id, owner, vin, license, make, model, year, mileage, location, velocity)

  def change_owner(self, owner, license):
    self.owner, self.license = owner, license

  def details(self):
    print (self.id,
     self.owner,
     self.vin,
     self.license,
     self.make,
     self.model,
     self.year,
     self.mileage,
     self.location,
     self.velocity)

@projection(Car)
class CarForPedestrian(object):
  FIELDS = ("location", "velocity")

car1 = Car(1,
           "Murugan",
           "SH1VA50N",
           "1MB4D",
           "Peacock",
           "single seater",
           "4000 BC",
           100000,
           "himalaya",
           299792458)

with CarForPedestrian(universe = dataframe([car1])) as cfp:
  for car in cfp.All():
    print "location, velocity:", car.location, car.velocity
    try:
      print "owner, license", car.owner, car.license
    except Exception:
      print "Other attributes are not there in projection"
    car.location, car.velocity = "western ghats", 299792459

car1.details()


