from pcc.attributes import dimension
from pcc.dataframe import dataframe
from pcc.subset import subset
from pcc.parameter import parameter
from pcc.join import join
from pcc.union import union

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

@parameter(CityWithOddDegree, ConnectedPath)
@subset(Path)
class PathsWithGivenCities(Path):
    ''' 
        Subclass of Path, 
        Class to find subgraph of given graph using only given vertices 
    '''
    @staticmethod
    def __query__(paths, cods, con_path):
        cities = set([cod.name for cod in cods])
        return [path for path in paths if PathsWithGivenCities.__predicate__(path, cities, con_path)]

    @staticmethod
    def __predicate__(path, cities, conpaths):
        return path.city1.name in cities and path.city2.name in cities and path not in conpaths

@parameter(PathsWithGivenCities)
@subset(Path)
class min_weight_perfect_match(Path):
    '''
        Subset of path, paths that make up the minimun weighted perfect matching edges
        using the given vertices.
    '''
    @staticmethod
    def __query__(paths, subgraph_paths):
        objs = maxWeightMatching([(int(p.city1.name), int(p.city2.name), 1-p.distance) for p in subgraph_paths], True)
        req_paths = [(str(i), str(objs[i])) for i in range(len(objs)) if objs[i] != -1]
        return [path for path in paths for req_path in req_paths if min_weight_perfect_match.__predicate__(path, req_path)]

    @staticmethod
    def __predicate__(path, req_path):
        return path.city1.name == req_path[0] and path.city2.name == req_path[1]

@union(min_weight_perfect_match, ConnectedPath)
class multigraph(Path):
    ''' A Union of two graphs '''
    pass

@parameter(City)
@subset(multigraph)
class EulerTour(Path):
    ''' 
        A subset of edges in order of the Euler Tour Route.
        Starting from the given vertex.
    '''
    @staticmethod
    def __query__(paths, cities):
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
                travel.append(next_path)
                city = next_path.city1 if city == next_path.city2 else next_path.city2
                visited.add(city.name)
                totake[city].remove(next_path)
            with dataframe() as df:
                if len(df.add(CitiesInTour, cities, params = (travel,))) == len(cities):
                    return travel
        return []

    @staticmethod
    def __predicate__():
        return True

    def display(self):
        print "Travelling from: %s to %s with cost %d" % (self.city1.name, self.city2.name, self.distance)

@parameter(EulerTour)
@subset(City)
class CitiesInTour(object):
    '''
        Subset of cities that are visited
        by the euler tour.
    '''
    @staticmethod
    def __predicate__(c, tour):
        for path in tour:
            if c.name in set([path.city1.name, path.city2.name]):
                return True
        return False

@join(EulerTour, Path)
class ShortenedEulerTour(Path):
    '''
        A join of Euler Tour and Path copied to the dimensions of Path (via inheritence)
        that represents the complete tour the travelling salesman takes.
    '''
    def __init__(self, path):
        self.city1, self.city2, self.distance = path.city1, path.city2, path.distance

    @staticmethod
    def __query__(euler_paths, all_paths):
        origin = [(path.city1, path.city2, path.distance) for path in euler_paths]
        all_path_dict = {}
        for path in all_paths:
            all_path_dict.setdefault(path.city1.name, {})[path.city2.name] = path

        reworked = []
        for i in range(len(euler_paths) - 1):
            city1, city2 = euler_paths[i].city1, euler_paths[i].city2
            nextcity1, nextcity2 = euler_paths[i + 1].city1, euler_paths[i + 1].city2
            if city1 == nextcity2:
                euler_paths[i + 1].city1, euler_paths[i + 1].city2 = (nextcity2, nextcity1)
                euler_paths[i].city1, euler_paths[i].city2 = (city2, city1)
            elif city2 == nextcity2:
                euler_paths[i + 1].city1, euler_paths[i + 1].city2 = (nextcity2, nextcity1)
            elif city1 == nextcity1:
                euler_paths[i].city1, euler_paths[i].city2 = (city2, city1)
            reworked.append(euler_paths[i])
        reworked.append(euler_paths[-1])
        seen = set()
        shortcut = []
        i = 0
        while i < len(reworked) - 1:
            city1 = reworked[i].city1
            city2 = reworked[i].city2
            seen.add(city1)
            if city2 not in seen:
                shortcut.append(reworked[i])
                seen.add(city2)
                i += 1
                continue
            while city2 in seen and i < len(reworked) - 1:
                i += 1
                city2 = reworked[i].city2
            if city2 not in seen:
                if city1.name in all_path_dict and city2.name in all_path_dict[city1.name]:
                    path = all_path_dict[city1.name][city2.name] 
                else:
                    path = all_path_dict[city2.name][city1.name]
                    path.city1, path.city2 = path.city2, path.city1
                shortcut.append(path)
                seen.add(city2)
                i += 1
        return shortcut

    def display(self):
        print "Travelling from: %s to %s with cost %d" % (self.city1.name, self.city2.name, self.distance)
                
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

def PrintConnections(paths):
    if paths == []:
        print "No Tours Found"
    for cp in paths:
        cp.display()

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
        with dataframe() as df:
            # Collect all Nodes that are part of the forest.
            connected_cities = df.add(ConnectedCity, cities)
            # If all nodes are part of the forest exit loop.
            if len(connected_cities) == len(cities):
                break
            # Find all paths that are have not been chosen.
            disconnected_paths = df.add(DisconnectedPath, paths)
            # Find all the paths that are not chosen 
            # but can be connected to the existing forest.
            all_nearby_discon_paths = df.add(NearByDisconnectedPath, disconnected_paths, params = (connected_cities,))
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
    with dataframe() as df:
        MST = df.add(ConnectedPath, paths)
        # Step 2: Find all cities in the MST that have odd degree (O)
        O = df.add(CityWithOddDegree, cities, params = (MST,))
        # Step 3: Find the induced subgraph given by the vertices from O
        subgraph = df.add(PathsWithGivenCities, paths, params = (O, MST))
        # Step 4: Find the Minimum weight perfect matching M in the subgraph
        M = df.add(min_weight_perfect_match, paths, params =(subgraph,))
        # Step 5: Combining the edges of M and T to form a connected multigraph H 
        # in which each vertex has even degree
        H = df.add(multigraph, M, MST)
        # Step 6: Form an Eulerian circuit in H.
        eulertour = df.add(EulerTour, H, params = (cities,))
        # Step 7: Making the circuit found in previous step into a Hamiltonian 
        # circuit by skipping repeated vertices (shortcutting).
        finaltour = df.add(ShortenedEulerTour, eulertour, paths)
        # Printing out the resulting tour.
        PrintConnections(finaltour)

# To generate random input
cities, paths = CreateRandomGraph(10)
# Executing the algorithm
PrintTSPPath(cities, paths)