'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from pcc.subset import subset
from pcc.parameter import parameter
from pcc.dataframe import dataframe
from pcc.attributes import dimension

from pygame.locals import *
import pygame, sys, os
from time import sleep
from threading import Thread, _sleep, Lock

carlock = Lock()
pedlock = Lock()
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
    @dimension(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(tuple)
    def position(self):
        return self._position

    @position.setter
    def position(self, value):
        self._position = value

    @dimension(tuple)
    def velocity(self):
        return self._velocity

    @velocity.setter
    def velocity(self, value):
        self._velocity = value

    def __init__(self, position):
        self.old_position = position
        self.position = position
        self.velocity = None
        self.ID = hash(self)
    
        
@subset(Car)
class InactiveCar(Car):
    @staticmethod
    def __predicate__(c):
        return c.velocity == (0, 0, 0) or c.velocity == None

    def Start(self):
        self.velocity = (Car.SPEED, 0, 0)

@subset(Car)
class ActiveCar(Car):
    @staticmethod
    def __predicate__(c):
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
    
    @dimension(str)
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    @dimension(int)
    def X(self):
        return self._X

    @X.setter
    def X(self, value):
        self._X = value

    @dimension(int)
    def Y(self):
        return self._Y

    @Y.setter
    def Y(self, value):
        self._Y = value

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
    def __predicate__(p):
        return p.X, p.Y == Pedestrian.INITIAL_POSITION

@subset(Pedestrian)
class Walker(Pedestrian):
    @staticmethod
    def __predicate__(p):
        return p.X, p.Y != Pedestrian.INITIAL_POSITION

@parameter(list)
@subset(Pedestrian)
class PedestrianInDanger(Pedestrian):
    @staticmethod
    def __predicate__(p, cars):
        for c in cars:
            cx, cy, cz = c.position
            if cy == p.Y and    abs(cx - p.X) < 70:
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
        with dataframe(carlock) as df:
            iacs = df.add(InactiveCar, cars)
            for car in iacs:
                car.Start()
                MainWindow.RegisterSpriteForRender(carsprites[car.ID])
                break
        _sleep(5)

def movecars(cars, MainWindow):
    while True:
        with dataframe(carlock) as df:
            acs = df.add(ActiveCar, cars)
            for car in acs:
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
        with dataframe(pedlock) as df:
            sps = df.add(StoppedPedestrian, peds)
            for ped in sps:
                MainWindow.RegisterSpriteForRender(pedsprites[ped.ID])
                ped.Move()
                break
        _sleep(3)

def movepeds(peds, cars, MainWindow):
    while True:
        with dataframe(pedlock) as df:
            pids = df.add(PedestrianInDanger, peds, params = (cars,))
            wks = df.add(Walker, peds)
            for pid in pids:
                pid.Avoid()
            for wk in wks:
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