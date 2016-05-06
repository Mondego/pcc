from pcc.subset import subset
from pcc.parameterize import parameterize
from pcc.dataframe import dataframe
from pcc.attributes import dimension
class Node(object):
  @dimension(int)
  def id(self):
    return self._id

  @id.setter
  def id(self, value):
    self._id = value
  
  @dimension(float)
  def pagerank(self):
    return self._pagerank

  @pagerank.setter
  def pagerank(self, value):
    self._pagerank = value

  def __init__(self, id, pagerank):
    self.id, self.pagerank = id, pagerank

class Edge(object):
  @dimension(Node)
  def start(self):
    return self._start

  @start.setter
  def start(self, value):
    self._start = value

  @dimension(Node)
  def end(self):
    return self._end

  @end.setter
  def end(self, value):
    self._end = value
  
  def __init__(self, n1, n2):
    self.start, self.end = (n1, n2)

@parameterize(Node)
@subset(Edge)
class InEdge(Edge):
  @staticmethod
  def __predicate__(e, n):
    return e.end.id == n.id

@parameterize(Node)
@subset(Edge)
class OutEdge(Edge):
  @staticmethod
  def __predicate__(e, n):
    return e.start.id == n.id

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
    with dataframe() as df:
      inedges_of_n = df.add(InEdge, edges, params = (n,))
      for inedge_of_n in inedges_of_n:
        pg_contributor = inedge_of_n.start
        outedges_of_contrib = df.add(OutEdge, edges, params = (pg_contributor,))
        sum += pg_contributor.pagerank / len(outedges_of_contrib)
    n.pagerank = ((1.0 - damp) / len(nodes)) + (damp * sum)
    diff = n.pagerank - old
    if diff > largest_change:
      largest_change = diff

for n in nodes:
  print n.id, n.pagerank
