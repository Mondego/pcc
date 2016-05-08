'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from pcc import dimension
from pcc import dataframe
from pcc import subset
from pcc import parameter
from pcc import join
from pcc import union
import pcc

import random
from mwmatching import maxWeightMatching

class City(object):
    ''' Class for a city '''
    @dimension(str)
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @dimension(bool)
    def isconnected(self):
        return self._isconnected

    @isconnected.setter
    def isconnected(self, value):
        self._isconnected = value

    def __init__(self, name):
        self.name = name
        self.isconnected = False


class Path(object):
    ''' Class for a connection between cities '''
    @dimension(City)
    def city1(self):
        return self._city1

    @city1.setter
    def city1(self, value):
        self._city1 = value

    @dimension(City)
    def city2(self):
        return self._city2

    @city2.setter
    def city2(self, value):
        self._city2 = value

    @dimension(float)
    def distance(self):
        return self._distance

    @distance.setter
    def distance(self, value):
        self._distance = value

    @dimension(bool)
    def isconnected(self):
        return self._isconnected

    @isconnected.setter
    def isconnected(self, value):
        self._isconnected = value

    def __eq__(self, obj):
        return (self.city1 == obj.city1
                        and self.city2 == obj.city2
                        and self.distance == obj.distance)

    def __init__(self, city1, city2, distance):
        self.city1 = city1
        self.city2 = city2
        self.distance = distance
        self.isconnected = False
        
@subset(Path)
class ConnectedPath(Path):
    ''' Subset of path, if this path between cities is chosen '''
    @staticmethod
    def __predicate__(p):
        return p.isconnected    

@subset(Path)
class DisconnectedPath(Path):
    ''' Subset of path, if this path between the cities is not chosen'''
    @staticmethod
    def __predicate__(p):
        return not p.isconnected

@subset(City)
class ConnectedCity(object):
    ''' Subset of cities that are connected by some chosen path. '''
    @staticmethod
    def __predicate__(c):
        return c.isconnected

@parameter(list) #connected_cities
@subset(DisconnectedPath.Class())
class NearByDisconnectedPath(Path):
    ''' 
     Subset of disconnected paths that can connect to one 
     among the connected cities given 
     in the parameter.
    '''
    @staticmethod
    def __predicate__(dis_path, connected_cities):
        set_cities = set([city.name for city in connected_cities])
        return (dis_path.city1.name in set_cities) ^ (dis_path.city2.name in set_cities)

@parameter(ConnectedPath)
@subset(City)
class CityWithOddDegree(object):
    ''' Subset of cities that have Odd degree in the Graph passed as a parameter '''
    @staticmethod
    def __query__(cities, paths):
        result = []
        for city in cities:
            pcs = [p for p in paths if city.name in set([p.city1.name, p.city2.name])]
            if CityWithOddDegree.__predicate__(pcs):
                result.append(city)
        return result

    @staticmethod
    def __predicate__(pcs):
        return len(pcs) % 2 == 1 

@parameter(CityWithOddDegree)
@subset(DisconnectedPath)
class PathsWithGivenCities(Path):
    ''' 
        Subclass of Path, 
        Class to find subgraph of given graph using only given vertices 
    '''
    @staticmethod
    def __query__(paths, cods):
        cities = set([cod.name for cod in cods])
        return [path for path in paths if PathsWithGivenCities.__predicate__(path, cities)]

    @staticmethod
    def __predicate__(path, cities):
        return path.city1.name in cities and path.city2.name in cities

@subset(PathsWithGivenCities)
class min_weight_perfect_match(Path):
    '''
        Subset of path, paths that make up the minimun weighted perfect matching edges
        using the given vertices.
    '''
    @staticmethod
    def __query__(subgraph_paths):
        objs = maxWeightMatching([(int(p.city1.name), int(p.city2.name), 1-p.distance) for p in subgraph_paths], True)
        req_paths = [(str(i), str(objs[i])) for i in range(len(objs)) if objs[i] != -1]
        return [path for path in subgraph_paths for req_path in req_paths if min_weight_perfect_match.__predicate__(path, req_path)]

    @staticmethod
    def __predicate__(path, req_path):
        return path.city1.name == req_path[0] and path.city2.name == req_path[1]

@union(min_weight_perfect_match, ConnectedPath)
class multigraph(Path):
    ''' A Union of two graphs '''
    pass

@parameter(multigraph)
@subset(City)
class EulerTour(object):
    ''' 
        A subset of cities in order of the Euler Tour Route.
    '''
    @staticmethod
    def __query__(cities, paths):
        for start_city in cities:
            totake = {}
            travel = []
            visited = set()
            for path in paths:
                totake.setdefault(path.city1, set()).add(path)
                totake.setdefault(path.city2, set()).add(path)
            city = [key for key in totake.keys() if key.name == start_city.name][0]
            visited.add(city.name)
            while len(totake[city]) > 0:
                next_path = totake[city].pop()
                travel.append(city)
                city = next_path.city1 if city == next_path.city2 else next_path.city2
                visited.add(city.name)
                totake[city].remove(next_path)
            travel.append(city)
            if len(set(travel)) == len(cities):
                return travel
        return []

    @staticmethod
    def __predicate__():
        return True

    def display(self):
        print "Travelling from: %s to %s with cost %d" % (self.city1.name, self.city2.name, self.distance)

@subset(EulerTour)
class HamiltonianTour(object):
    '''
        A subset of cities that represents the 
        complete tour the travelling salesman takes.
    '''
    def __init__(self, path):
        self.city1, self.city2, self.distance = path.city1, path.city2, path.distance

    @staticmethod
    def __query__(cities):
        seen = set()
        result = []
        for city in cities:
            if HamiltonianTour.__predicate__(city, seen):
                result.append(city)
                seen.add(city)
        return result

    @staticmethod
    def __predicate__(city, seen):
        return city not in seen
                
############ I/O Functions #################
def CreateRandomGraph(number):
    cities = [City(str(i)) for i in range(number)]
    matrix = [[0 for i in range(number)] for j in range(number)]
    for i in range(number):
        for j in range(i + 1, number):
            matrix[i][j] = matrix[j][i] = random.randint(1, number)
    paths = [Path(cities[i], cities[j], matrix[i][j]) 
                        for i in range(number) 
                            for j in range(i + 1, number) 
                                if i!=j]
    return cities, paths

def PrintConnections(cities):
    if cities == []:
        print "No Tours Found"
    for cp in cities:
        print "Traveling to city: ", cp.name

def PrintPath(paths):
    for p in paths:
        print p.city1.name, p.city2.name, p.distance

############ I/O Functions End #################

################ Algorithm #####################

def BuildMst(cities, paths):
    # Treat a random node as the start, 
    # Make it a forest with one node.
    randomstart = random.choice(cities)
    randomstart.isconnected = True
    while True:
        # Collect all Nodes that are part of the forest.
        connected_cities = pcc.create(ConnectedCity, cities)
        # If all nodes are part of the forest exit loop.
        if len(connected_cities) == len(cities):
            break
        # Find all paths that are have not been chosen.
        disconnected_paths = pcc.create(DisconnectedPath, paths)
        # Find all the paths that are not chosen 
        # but can be connected to the existing forest.
        all_nearby_discon_paths = pcc.create(NearByDisconnectedPath, disconnected_paths, params = (connected_cities,))
        if len(all_nearby_discon_paths) != 0:
            # Choose the path with the least weight.
            best_choice_path = sorted(all_nearby_discon_paths, key = lambda x: x.distance)[0]
            # Join it to the forest.
            best_choice_path.isconnected = True
            best_choice_path.city1.isconnected = True
            best_choice_path.city2.isconnected = True

def PrintTSPPath(cities, paths):
    # Step 1: Building a Minimun spanning tree using Prim's Algorithm
    BuildMst(cities, paths)
    
    MST = pcc.create(ConnectedPath, paths)
    # Step 2: Find all cities in the MST that have odd degree (O)
    O = pcc.create(CityWithOddDegree, cities, params = (MST,))
    # Step 3: Find the induced subgraph given by the vertices from O
    not_MST_paths = pcc.create(DisconnectedPath, paths)
    subgraph = pcc.create(PathsWithGivenCities, not_MST_paths, params = (O,))
    # Step 4: Find the Minimum weight perfect matching M in the subgraph
    M = pcc.create(min_weight_perfect_match, subgraph)
    # Step 5: Combining the edges of M and T to form a connected multigraph H 
    # in which each vertex has even degree
    H = pcc.create(multigraph, M, MST)
    # Step 6: Form an Eulerian circuit in H.
    eulertour = pcc.create(EulerTour, cities, params = (H,))
    # Step 7: Making the circuit found in previous step into a Hamiltonian 
    # circuit by skipping repeated vertices (shortcutting).
    finaltour = pcc.create(HamiltonianTour, eulertour)
    # Printing out the resulting tour.
    PrintConnections(finaltour)

# To generate random input
cities, paths = CreateRandomGraph(10)
# Executing the algorithm
PrintTSPPath(cities, paths)