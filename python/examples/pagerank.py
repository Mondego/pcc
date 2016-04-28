from pcc.subset import subset
from pcc.parameterize import parameterize
from pcc.dataframe import dataframe

class Node(object):
  def __init__(self, id, pagerank):
    self.id, self.pagerank = id, pagerank

class Edge(object):
  def __init__(self, n1, n2):
    self.n1, self.n2 = (n1, n2)

@parameterize(Node)
@subset(Edge)
class InEdge(Edge):
  @staticmethod
  def __query__(edges, n):
    return [e
      for e in edges
      if InEdge.__predicate__(e) and e.n2.id == n.id]

  @staticmethod
  def __predicate__(e):
    return True

@parameterize(Node)
@subset(Edge)
class OutEdge(Edge):
  @staticmethod
  def __query__(edges, n):
    return [e
      for e in edges
      if OutEdge.__predicate__(e) and e.n1.id == n.id]

  @staticmethod
  def __predicate__(e):
    return True

def CreateNodesAndEdges():
  nodes = []
  edges = []
  for i in range(4):
    nodes.append(Node(i, 0.25))

  edges.append(Edge(nodes[0],nodes[1]))
  edges.append(Edge(nodes[0],nodes[2]))
  edges.append(Edge(nodes[0],nodes[3]))
  edges.append(Edge(nodes[1],nodes[2]))
  edges.append(Edge(nodes[3],nodes[2]))
  return nodes, edges

nodes, edges = CreateNodesAndEdges()
largest_change = 100
allowed_delta = 0.001
damp = 0.85

while largest_change > allowed_delta:
  largest_change = 0.0
  for n in nodes:
    old = n.pagerank
    sum = 0.0
    df = dataframe(edges)
    with InEdge(universe = df, params = (n,)) as ies:
      for ie in ies.All():
        contributor = ie.n1
        with OutEdge(universe = df, params = (contributor,), nomerge = True) as oes:
          sum += contributor.pagerank / len(oes.All())
    n.pagerank = ((1.0 - damp) / len(nodes)) + (damp * sum)
    diff = n.pagerank - old
    if diff > largest_change:
      largest_change = diff

for n in nodes:
  print n.id, n.pagerank
