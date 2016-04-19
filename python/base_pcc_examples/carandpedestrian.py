﻿from pcc.subset import subset
from pcc.parameterize import parameterize
from pcc.dataframe import dataframe
from pygame.locals import *
import pygame, sys, os
from time import sleep
from threading import Thread, _sleep

def load_image(fullname, colorkey=None):
  try:
    image = pygame.image.load(fullname)
  except pygame.error, message:
    print 'Cannot load image:', fullname
    raise SystemExit, message
  image = image.convert()
  if colorkey is not None:
    if colorkey is -1:
      colorkey = image.get_at((0,0))
    image.set_colorkey(colorkey, RLEACCEL)
  return image, image.get_rect()

class CarSprite(pygame.sprite.Sprite):
  def __init__(self, car):
    self.car = car
    pygame.sprite.Sprite.__init__(self) 
    self.image, self.rect = load_image('images/car-small.gif',-1)
   
  def update(self):
    oldx, oldy, oldz = self.car.old_position
    x, y, z = self.car.position
    if x != oldx or y != oldy:
      self.rect.move_ip((x - oldx, y - oldy))
      self.car.old_position = self.car.position
  

class Car(object):
  FINAL_POSITION = 500
  SPEED = 10
  def __init__(self, position):
    self.old_position = position
    self.position = position
    self.velocity = None
    self.ID = hash(self)
  
    
@subset(Car)
class InactiveCar(Car):
  @staticmethod
  def __query__(cars):
    return [c 
     for c in cars
     if InactiveCar.__invariant__(c)]

  @staticmethod
  def __invariant__(c):
    return c.velocity == (0, 0, 0) or c.velocity == None

  def Start(self):
    self.velocity = (Car.SPEED, 0, 0)

@subset(Car)
class ActiveCar(Car):
  @staticmethod
  def __query__(cars):
    return [c 
     for c in cars
     if ActiveCar.__invariant__(c)]

  @staticmethod
  def __invariant__(c):
    return not (c.velocity == (0, 0, 0) or c.velocity == None)

  def Move(self):
    x,y,z = self.position
    xvel, yvel, zvel = self.velocity
    self.position = (x + xvel, y + yvel, z + zvel)
    
  def Stop(self):
    self.position, self.velocity = (0,0,0), (0,0,0)

class PedestrianSprites(pygame.sprite.Sprite):
  def __init__(self, ped):
    self.ped = ped
    pygame.sprite.Sprite.__init__(self) 
    self.image, self.rect = load_image('images/man-walking-small.gif',-1)
    self.rect.move_ip((self.ped.X, self.ped.Y))

  def update(self):
    if self.ped.X != self.ped.oldX or self.ped.Y != self.ped.oldY:
      self.rect.move_ip((self.ped.X - self.ped.oldX, self.ped.Y - self.ped.oldY)) 
      self.ped.oldX, self.ped.oldY = self.ped.X, self.ped.Y
          
class Pedestrian(object):
  INITIAL_POSITION = (400, 0)
  SPEED = 10
  def __init__(self):
    self.ID = hash(self)
    self.X, self.Y = Pedestrian.INITIAL_POSITION
    self.oldX, self.oldY = Pedestrian.INITIAL_POSITION

  def Move(self):
    self.X -= Pedestrian.SPEED
    if self.X <= 0:
      self.Stop()

  def Stop(self):
    self.X, self.Y = Pedestrian.INITIAL_POSITION

  def SetPosition(self, x):
    self.X = x

@subset(Pedestrian)
class StoppedPedestrian(Pedestrian):
  @staticmethod
  def __query__(pedestrians):
    return [p 
     for p in pedestrians
     if StoppedPedestrian.__invariant__(p)]

  @staticmethod
  def __invariant__(p):
    return p.X, p.Y == Pedestrian.INITIAL_POSITION

@subset(Pedestrian)
class Walker(Pedestrian):
  @staticmethod
  def __query__(pedestrians):
    return [p 
     for p in pedestrians
     if Walker.__invariant__(p)]

  @staticmethod
  def __invariant__(p):
    return p.X, p.Y != Pedestrian.INITIAL_POSITION

@parameterize
@subset(Pedestrian)
class PedestrianInDanger(Pedestrian):
  @staticmethod
  def __query__(pedestrians, cars):
    return [p 
     for p in pedestrians
     if PedestrianInDanger.__invariant__(p, cars)]

  @staticmethod
  def __invariant__(p, cars):
    for c in cars:
      cx, cy, cz = c.position
      if cy == p.Y and  abs(cx - p.X) < 70:
        return True
    return False

  def Avoid(self):
    self.Y += 50

class PyManMain(object):
  """The Main PyMan Class - This class handles the main 
  initialization and creating of the Game."""
  
  def __init__(self, width=640,height=480):
    """Initialize"""
    """Initialize PyGame"""
    pygame.init()
    """Set the window Size"""
    self.width = width
    self.height = height
    """Create the Screen"""
    self.screen = pygame.display.set_mode((self.width
                                          ,self.height))
    self.PresentSprites = []
    self.NewSprites = []

  def MainLoop(self):
    """This is the Main Loop of the Game"""
    self.background = pygame.Surface(self.screen.get_size())
    self.background = self.background.convert()
    self.background.fill((255,255,255))
    while True:
      self.screen.blit(self.background, (0, 0))
      self.LoadNewSprites()
      for sp in self.PresentSprites:
        sp.update()
      groups = pygame.sprite.RenderPlain(tuple(self.PresentSprites))
      groups.draw(self.screen)
      pygame.display.flip()
      events = pygame.event.get(pygame.QUIT)
      if events:
        sys.exit(0)
      _sleep(0.5)

  def LoadNewSprites(self):
    if self.NewSprites:
      self.PresentSprites += self.NewSprites
      self.NewSprites = []

  def RegisterSpriteForRender(self, sp):
    self.NewSprites.append(sp)


def startEngines(cars, MainWindow, carsprites):
  while True:
    with InactiveCar(universe = dataframe(cars)) as iacs:
      for car in iacs.All():
        car.Start()
        MainWindow.RegisterSpriteForRender(carsprites[car.ID])
        break
    _sleep(5)

def movecars(cars, MainWindow):
  while True:
    with ActiveCar(universe = dataframe(cars)) as acs:
      for car in acs.All():
        car.Move()
        _sleep(0.3)

def CruiseControl(cars, MainWindow):
  carsprites = dict([(car.ID, CarSprite(car)) for car in cars]) 
  engines = Thread(target = startEngines, args = (cars, MainWindow, carsprites))
  engines.daemon = True
  engines.start()

  drivetrain = Thread(target = movecars, args = (cars, MainWindow))
  drivetrain.daemon = True
  drivetrain.start()

  engines.join()
  drivetrain.join()

def startWalking(peds, MainWindow, pedsprites):
  while True:
    with StoppedPedestrian(universe = dataframe(peds)) as sps:
      for ped in sps.All():
        MainWindow.RegisterSpriteForRender(pedsprites[ped.ID])
        ped.Move()
        break
    _sleep(3)

def movepeds(peds, cars, MainWindow):
  while True:
    with PedestrianInDanger(universe = dataframe(peds), params = (cars,)) as pids, Walker(universe = dataframe(peds)) as wks:
      for pid in pids.All():
        pid.Avoid()
      for wk in wks.All():
        wk.Move()
    _sleep(0.5)

def WalkingControl(peds, cars, MainWindow):
  pedsprites = dict([(ped.ID, PedestrianSprites(ped)) for ped in peds]) 
  walkers = Thread(target = startWalking, args = (peds, MainWindow, pedsprites))
  walkers.daemon = True
  walkers.start()

  pedmovers = Thread(target = movepeds, args = (peds, cars, MainWindow))
  pedmovers.daemon = True
  pedmovers.start()

  walkers.join()
  pedmovers.join()



if __name__ == "__main__":
  MainWindow = PyManMain()
  cars = [Car((0,0,0)), Car((0,0,0)), Car((0,0,0)), Car((0,0,0))]
  carController = Thread(target = CruiseControl, args = (cars, MainWindow))
  carController.daemon = True
  
  pedestrians = [Pedestrian()]
  pedController = Thread(target = WalkingControl, args = (pedestrians, cars, MainWindow))
  pedController.daemon = True
  
  gfx = Thread(target = MainWindow.MainLoop)
  gfx.daemon = True

  gfx.start()
  carController.start()
  pedController.start()
  
  gfx.join()