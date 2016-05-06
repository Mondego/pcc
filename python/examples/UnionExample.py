from pcc.dataframe import dataframe
from pcc.union import union
from pcc.attributes import dimension

class Orange(object):
  @dimension(int)
  def volume(self):
    return self._volume

  @volume.setter
  def volume(self, value):
    self._volume = value

  @dimension(bool)
  def zest(self):
    return self._zest

  @zest.setter
  def zest(self, value):
    self._zest = value

  @dimension(int)
  def pulp(self):
    return self._pulp

  @pulp.setter
  def pulp(self, value):
    self._pulp = value

  def __init__(self, volume, zest, pulp):
    self.volume, self.pulp, self.zest = volume, pulp, zest

  def details(self):
    print self.volume, self.zest, self.pulp

class Mango(object):
  @dimension(int)
  def volume(self):
    return self._volume

  @volume.setter
  def volume(self, value):
    self._volume = value

  @dimension(bool)
  def origin(self):
    return self._origin

  @origin.setter
  def origin(self, value):
    self._origin = value

  @dimension(int)
  def pulp(self):
    return self._pulp

  @pulp.setter
  def pulp(self, value):
    self._pulp = value

  def __init__(self, volume, origin, pulp):
    self.volume, self.pulp, self.origin= volume, pulp, origin

  def details(self):
    print self.volume, self.origin, self.pulp

class Lemon(object):
  @dimension(int)
  def volume(self):
    return self._volume

  @volume.setter
  def volume(self, value):
    self._volume = value

  @dimension(bool)
  def zest(self):
    return self._zest

  @zest.setter
  def zest(self, value):
    self._zest = value

  @dimension(int)
  def size(self):
    return self._size

  @size.setter
  def size(self, value):
    self._size = value

  def __init__(self, volume, zest, size):
    self.volume, self.size, self.zest = volume, size, zest

  def details(self):
    print self.volume, self.size, self.zest

@union(Orange, Mango, Lemon)
class Fruit(object):
  def juice(self):
    self.volume /= 2


o1 = Orange(100, 10, 20)
o2 = Orange(60, 10, 10)
o3 = Orange(80, 10, 10)
o4 = Orange(200, 10, 60)
m1 = Mango(200, "India", 50)
m2 = Mango(20, "Peru", 40)
m3 = Mango(100, "India", 50)
l1 = Lemon(20, 10, 2)
l2 = Lemon(40, 20, 4)

with dataframe() as df:
  fruits = df.add(Fruit, [o1, o2, o3, o4], [m1, m2, m3], [l1, l2])
  for fr in fruits:
    fr.juice()

for obj in [o1, o2, o3, o4] + [m1, m2, m3] + [l1, l2]:
  obj.details()