from dependent_classes.subset import Subset
from dependent_classes.parameterize import Parameterize
from pygame.locals import *
import pygame, sys, os
from time import sleep
from threading import Thread

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
    if x != oldx and y != oldy:
      self.rect.move_ip((x - old_x, y - old_y)) 
      self.car.old_position = self.car.position

class Car(object):
  FINAL_POSITION = 500
  SPEED = 40
  def __init__(self, position):
    self.old_position = position
    self.position = position
    self.velocity = None
    self.id = hash(self)
  
    
@Subset(Car)
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

@Subset(Car)
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

  def update(self):
    if self.ped.X != self.ped.oldX and self.ped.Y != self.ped.oldY:
      self.rect.move_ip((self.ped.X - self.ped.oldX, self.ped.Y - self.ped.oldY)) 
      self.ped.oldX, self.ped.oldY = self.ped.X, self.ped.Y
          
class Pedestrian(object):
  INITIAL_POSITION = (650, 0)
  SPEED = 10
  def __init__(self):
    self.ID = hash(self)
    self.X, self.Y = INITIAL_POSITION
    self.oldX, self.oldY = INITIAL_POSITION
    return super(Pedestrian, self).__init__(*groups)

  def Move(self):
    self.X -= SPEED
    if self.X <= 0:
      self.Stop()

  def Stop(self):
    self.X, self.Y = INITIAL_POSITION

  def SetPosition(self, x):
    self.X = x

@Subset(Pedestrian)
class StoppedPedestrian(Pedestrian):
  @staticmethod
  def __query__(pedestrians):
    return [p 
     for p in pedestrians
     if StoppedPedestrian.__invariant__(p)]

  @staticmethod
  def __invariant__(p):
    return p.X, p.Y == Pedestrian.INITIAL_POSITION

@Subset(Pedestrian)
class Walker(Pedestrian):
  @staticmethod
  def __query__(pedestrians):
    return [p 
     for p in pedestrians
     if Walker.__invariant__(p)]

  @staticmethod
  def __invariant__(p):
    return p.X, p.Y != Pedestrian.INITIAL_POSITION

@Parameterize
@Subset(Pedestrian)
class PedestrianInDanger(Pedestrian):
  @staticmethod
  def __query__(pedestrians, cars):
    return [p 
     for p in pedestrians
     if Walker.__invariant__(p, cars)]

  @staticmethod
  def __invariant__(p, cars):
    for c in cars:
      cx, cy, cz = c.position
      if cy == p.Y and  abs(cx - p.X) < 130:
        return True
    return False

  def Move(self):
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
      self.LoadNewSprites()
      for sp in self.PresentSprites:
        sp.update()
      pygame.display.flip()
      events = pygame.event.get(pygame.QUIT)
      if events:
        sys.exit(0)
      sleep(0.2)

  def LoadNewSprites(self):
    if self.NewSprites:
      groups = pygame.sprite.RenderPlain(tuple(self.NewSprites))
      groups.draw(self.screen)
      self.PresentSprites += self.NewSprites
      self.NewSprites = []

  def RegisterSpriteForRender(self, sp):
    self.NewSprites.append(sp)


def startEngines(cars, MainWindow):
  with InactiveCar(universe = cars) as iacs:
    for car in iacs.All():
      car.Start()
      MainWindow.RegisterSpriteForRender(CarSprite(car))
      sleep(3)

def movecars(cars, MainWindow):
  while True:
    with ActiveCar(universe = cars) as acs:
      for car in acs.All():
        car.Move()
        sleep(0.2)

def CruiseControl(cars, MainWindow):
  engines = Thread(target = startEngines, args = (cars, MainWindow))
  engines.daemon = True
  engines.start()

  drivetrain = Thread(target = movecars, args = (cars, MainWindow))
  drivetrain.daemon = True
  drivetrain.start()

  engines.join()
  drivetrain.join()



if __name__ == "__main__":
  MainWindow = PyManMain()
  cars = [Car((0,0,0))] * 1
  carController = Thread(target = CruiseControl, args = (cars, MainWindow))
  carController.daemon = True
  
  #pedestrians = [Pedestrian()] * 4
  #pedController = Thread(target = WalkingControl, args = (pedestrians, MainWindow))
  #pedController.daemon = True
  
  gfx = Thread(target = MainWindow.MainLoop)
  gfx.daemon = True

  gfx.start()
  carController.start()
  #pedController.start()
  
  gfx.join()