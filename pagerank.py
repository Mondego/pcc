from dependent_classes.subset import Subset
from dependent_classes.parameterize import Parameterize

class Node(object):
  def __init__(self, id, pagerank):
    self.id, self.pagerank = id, pagerank

class Edge(object):
  def __init__(self, n1, n2):
    self.n1, self.n2 = (n1, n2)

@Parameterize
@Subset(Edge)
class InEdge(Edge):
  @staticmethod
  def __query__(edges, n):
    return [e
      for e in edges
      if InEdge.__invariant__(e) and e.n2 == n.id]

  @staticmethod
  def __invariant__(e):
    return True

@Parameterize
@Subset(Edge)
class OutEdge(Edge):
  @staticmethod
  def __query__(edges, n):
    return [e
      for e in edges
      if OutEdge.__invariant__(e) and e.n1 == n.id]

  @staticmethod
  def __invariant__(e):
    return True

def CreateNodesAndEdges():
  nodes = []
  edges = []
  for i in range(4):
    nodes.append(Node(i, 0.25))

  # Very problematic because of copy semantics.
  #edges.append(Edge(nodes[0],nodes[1]))
  #edges.append(Edge(nodes[0],nodes[2]))
  #edges.append(Edge(nodes[0],nodes[3]))
  #edges.append(Edge(nodes[1],nodes[2]))
  #edges.append(Edge(nodes[3],nodes[2]))
  edges.append(Edge(0,1))
  edges.append(Edge(0,2))
  edges.append(Edge(0,3))
  edges.append(Edge(1,2))
  edges.append(Edge(3,2))

  return nodes, edges

def GetNodeFromId(nodes, id):
  for n in nodes:
    if n.id == id:
      return n

nodes, edges = CreateNodesAndEdges()
largest_change = 100
allowed_delta = 0.001
damp = 0.85

while largest_change > allowed_delta:
  largest_change = 0.0
  for n in nodes:
    old = n.pagerank
    sum = 0.0
    with InEdge(universe = edges, params = (n,)) as ies:
      for ie in ies.All():
        contributor = GetNodeFromId(nodes, ie.n1)
        with OutEdge(universe = edges, params = (contributor,)) as oes:
          sum += contributor.pagerank / len(oes.All())
    n.pagerank = ((1.0 - damp) / len(nodes)) + (damp * sum)
    diff = n.pagerank - old
    if diff > largest_change:
      largest_change = diff

for n in nodes:
  print n.id, n.pagerank
