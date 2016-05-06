# Example of kNN implemented using PCCs in Python
 
import csv
import random
import math

from pcc.attributes import dimension
from pcc.dataframe import dataframe
from pcc.join import join
from pcc.subset import subset
from pcc.parameterize import parameterize

class flower(object):
   @dimension(float)
   def sepal_length(self):
     return self._sepal_length
   
   @sepal_length.setter
   def sepal_length(self, value):
     self._sepal_length = value

   @dimension(float)
   def sepal_width(self):
     return self._sepal_width
   
   @sepal_width.setter
   def sepal_width(self, value):
     self._sepal_width = value

   @dimension(float)
   def petal_length(self):
     return self._petal_length
   
   @petal_length.setter
   def petal_length(self, value):
     self._petal_length = value

   @dimension(float)
   def petal_width(self):
     return self._petal_width
   
   @petal_width.setter
   def petal_width(self, value):
     self._petal_width = value

   @dimension(str)
   def fl_type(self):
     return self._fl_type
   
   @fl_type.setter
   def fl_type(self, value):
     self._fl_type = value

   def __init__(self, sl, sw, pl, pw, tp):
     self.sepal_length = float(sl)
     self.sepal_width = float(sw)
     self.petal_length = float(pl)
     self.petal_width = float(pw)
     self.fl_type = tp

@parameterize(int)
@join(flower, flower)
class k_nearest_neighbours(object):
  @dimension(flower)
  def test_flower(self):
    return self._test_flower

  @test_flower.setter
  def test_flower(self, value):
    self._test_flower = value

  @dimension(list)
  def training_flowers(self):
    return self._training_flowers

  @training_flowers.setter
  def training_flowers(self, value):
    self._training_flowers = value

  def getResponse(self):
    classVotes = {}
    for x in range(len(self.training_flowers)):
      fl, dist = self.training_flowers[x]
      response = fl.fl_type
      classVotes[response] = classVotes.setdefault(response, 0) + 1 
      
    sortedVotes = sorted(classVotes.iteritems(), key = lambda x: x[1], reverse = True)
    return sortedVotes[0][0]

  @staticmethod
  def euclideanDistance(fl1, fl2):
    return math.sqrt(pow((fl1.sepal_length - fl2.sepal_length), 2)
                     + pow((fl1.sepal_width - fl2.sepal_width), 2)
                     + pow((fl1.petal_length - fl2.petal_length), 2)
                     + pow((fl1.petal_width - fl2.petal_width), 2))
    
  def __init__(self, test_fl, training_fls):
    self.test_flower = test_fl
    self.training_flowers = training_fls

  @staticmethod
  def __query__(test_flowers, training_flowers, k):
    results = []
    for te_f in test_flowers:
      k_nearest = sorted([(tr_f, k_nearest_neighbours.euclideanDistance(te_f, tr_f)) 
                          for tr_f in training_flowers], key = lambda x: x[1])[:k]
      if k_nearest_neighbours.__predicate__(te_f, k_nearest, k):
        results.append((te_f, k_nearest))

    return results
  @staticmethod
  def __predicate__(test_fl, training_fls, k):
    return sorted(training_fls, key = lambda x: x[1]) == training_fls and len(training_fls) <= k


def loadDataset(filename, split, trainingSet=[] , testSet=[]):
  with open(filename, 'rb') as csvfile:
    lines = csv.reader(csvfile)
    dataset = list(lines)
    for x in range(len(dataset)-1):
      fl = flower(*dataset[x][:5])
      if random.random() < split:
        trainingSet.append(fl)
      else:
        testSet.append(fl)

def getAccuracy(all_k_nearest, predictions):
  correct = 0
  for x in range(len(all_k_nearest)):
    if all_k_nearest[x].test_flower.fl_type == predictions[x]:
      correct += 1
  return (correct/float(len(all_k_nearest))) * 100.0

def main():
  trainingSet = []
  testSet = []
  split = 0.67
  loadDataset("iris.data", split, trainingSet, testSet)
  print 'Train set: ' + repr(len(trainingSet))
  print 'Test set: ' + repr(len(testSet))
  predictions = []
  k = 3
  with dataframe() as df:
    all_k_nearest = df.add(k_nearest_neighbours, testSet, trainingSet, params = (k,))
    for k_nearest in all_k_nearest:
      result = k_nearest.getResponse()
      predictions.append(result)
      print('> predicted=' + repr(result) + ', actual=' + repr(k_nearest.test_flower.fl_type))
    accuracy = getAccuracy(all_k_nearest, predictions)
  print('Accuracy: ' + repr(accuracy) + '%')

main()