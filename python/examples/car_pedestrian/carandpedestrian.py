'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from pcc import subset
from pcc import parameter
from pcc import dataframe
from pcc import dimension

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

class Car(pygame.sprite.Sprite):
    # The class that shows the image of a car.
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
        self.old_position = position # Old position is not a dimension.
        self.position = position
        self.velocity = None
        self.ID = hash(self)
        pygame.sprite.Sprite.__init__(self) 
        self.image, self.rect = load_image('images/car-small.gif',-1) # Not dimensions

    def update(self):
        oldx, oldy, oldz = self.old_position
        x, y, z = self.position
        if x != oldx or y != oldy:
            self.rect.move_ip((x - oldx, y - oldy))
            self.old_position = self.position
    

@subset(Car)
class InactiveCar(Car):
    # Car that is not moving, Velocity is zero
    @staticmethod
    def __predicate__(c):
        return c.velocity == (0, 0, 0) or c.velocity == None

    def Start(self):
        self.velocity = (Car.SPEED, 0, 0)

@subset(Car)
class ActiveCar(Car):
    # car that is moving, velocity is not zero
    @staticmethod
    def __predicate__(c):
        return not (c.velocity == (0, 0, 0) or c.velocity == None)

    def Move(self):
        x,y,z = self.position
        xvel, yvel, zvel = self.velocity
        self.position = (x + xvel, y + yvel, z + zvel)
        
    def Stop(self):
        self.position, self.velocity = (0,0,0), (0,0,0)

class Pedestrian(pygame.sprite.Sprite):
# base pedestrian class 
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
        self.oldX, self.oldY = Pedestrian.INITIAL_POSITION # not dimensions
        pygame.sprite.Sprite.__init__(self) 
        self.image, self.rect = load_image('images/man-walking-small.gif',-1) # Not dimensions
        self.rect.move_ip((self.X, self.Y))

    def Move(self):
        self.X -= Pedestrian.SPEED
        if self.X <= 0:
            self.Stop()

    def Stop(self):
        self.X, self.Y = Pedestrian.INITIAL_POSITION

    def SetPosition(self, x):
        self.X = x

    def update(self):
        if self.X != self.oldX or self.Y != self.oldY:
            self.rect.move_ip((self.X - self.oldX, self.Y - self.oldY)) 
            self.oldX, self.oldY = self.X, self.Y
                    

    
@subset(Pedestrian)
class StoppedPedestrian(Pedestrian):
    # A person that is not moving
    @staticmethod
    def __predicate__(p):
        return p.X, p.Y == Pedestrian.INITIAL_POSITION

@subset(Pedestrian)
class Walker(Pedestrian):
    # A person who is walking.
    @staticmethod
    def __predicate__(p):
        return p.X, p.Y != Pedestrian.INITIAL_POSITION

@parameter(list)
@subset(Pedestrian)
class PedestrianInDanger(Pedestrian):
    # A person who is in danger of colliding with a car
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
    # Launches the GFX window for the application
    # Renders the cars, and pedestrians, and animates their
    # movements based on the position in the car.
    def __init__(self, width=640,height=480):
        """Initialize"""
        """Initialize PyGame"""
        pygame.init()
        """Set the window Size"""
        self.width = width
        self.height = height
        """Create the Screen"""
        self.screen = pygame.display.set_mode((self.width, self.height))
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
            _sleep(1)

    def LoadNewSprites(self):
        if self.NewSprites:
            self.PresentSprites += self.NewSprites
            self.NewSprites = []

    def RegisterSpriteForRender(self, sp):
        self.NewSprites.append(sp)

def register(id, items, MainWindow):
    for item in items:
        if item.ID == id:
            MainWindow.RegisterSpriteForRender(item)
            return

def StartInactiveCars(cars, MainWindow):
    # Starts inactive cars every 5 secs
    while True:
        with dataframe(carlock) as df:
            iacs = df.add(InactiveCar, cars)
            for car in iacs:
                car.Start()
                register(car.ID, cars, MainWindow)
                break
        _sleep(5)

def MoveActiveCars(cars, MainWindow):
    # Moves active cars every 300 ms
    while True:
        with dataframe(carlock) as df:
            acs = df.add(ActiveCar, cars)
            for car in acs:
                car.Move()
                _sleep(0.3)

def CarWorkerControl(cars, MainWindow):
    # Starts the car related threads
    start_car_thread = Thread(target = StartInactiveCars, args = (cars, MainWindow))
    start_car_thread.daemon = True
    start_car_thread.start()

    move_car_thread = Thread(target = MoveActiveCars, args = (cars, MainWindow))
    move_car_thread.daemon = True
    move_car_thread.start()

    start_car_thread.join()
    move_car_thread.join()

def StartPedestrian(peds, MainWindow):
    # Make a pedestrian walk every 3 secs.
    while True:
        with dataframe(pedlock) as df:
            for ped in df.add(StoppedPedestrian, peds):
                register(ped.ID, peds, MainWindow)
                ped.Move()
                break
        _sleep(3)

def MovePedestrian(peds, cars, MainWindow):
    # Nake a Walker move.
    while True:
        with dataframe(pedlock) as df:
            pids = df.add(PedestrianInDanger, peds, params = (cars,))
            wks = df.add(Walker, peds)
            for pid in pids:
                pid.Avoid()
            for wk in wks:
                wk.Move()
        _sleep(0.5)

def PedestrianWorkerControl(peds, cars, MainWindow):
    # starts the pedestrian related threads
    start_ped_thread = Thread(target = StartPedestrian, args = (peds, MainWindow))
    start_ped_thread.daemon = True
    start_ped_thread.start()

    move_walker_thread = Thread(target = MovePedestrian, args = (peds, cars, MainWindow))
    move_walker_thread.daemon = True
    move_walker_thread.start()

    start_ped_thread.join()
    move_walker_thread.join()



if __name__ == "__main__":
    MainWindow = PyManMain()
    cars = [Car((0,0,0)), Car((0,0,0)), Car((0,0,0)), Car((0,0,0))]
    carController = Thread(target = CarWorkerControl, args = (cars, MainWindow))
    carController.daemon = True
    
    pedestrians = [Pedestrian()]
    pedController = Thread(target = PedestrianWorkerControl, args = (pedestrians, cars, MainWindow))
    pedController.daemon = True
    
    gfx = Thread(target = MainWindow.MainLoop)
    gfx.daemon = True

    gfx.start()
    carController.start()
    pedController.start()
    gfx.join()