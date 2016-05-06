# Example of kNN implemented using PCCs in Python
 
import csv
import random
import math

from threading import Thread

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

   @dimension(str)
   def predicted_type(self):
     return self._predicted_type
   
   @predicted_type.setter
   def predicted_type(self, value):
     self._predicted_type = value

   def __init__(self, sl, sw, pl, pw, tp):
     self.sepal_length = float(sl)
     self.sepal_width = float(sw)
     self.petal_length = float(pl)
     self.petal_width = float(pw)
     self.fl_type = tp
     self.predicted_type = None

@parameterize(flower, int)
@subset(flower)
class knn(object):
  @staticmethod
  def euclideanDistance(fl1, fl2):
    return math.sqrt(pow((fl1.sepal_length - fl2.sepal_length), 2)
                     + pow((fl1.sepal_width - fl2.sepal_width), 2)
                     + pow((fl1.petal_length - fl2.petal_length), 2)
                     + pow((fl1.petal_width - fl2.petal_width), 2))
  
  @staticmethod
  def __query__(training_flowers, test, k):
    return sorted([tr_f for tr_f in training_flowers], key = lambda x: knn.euclideanDistance(test, x))[:k]
    
  @staticmethod
  def __predicate__():
    return True

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

def getResponse(knns):
    classVotes = {}
    for one_neighbour in knns:
      response = one_neighbour.fl_type
      classVotes[response] = classVotes.setdefault(response, 0) + 1 
      
    sortedVotes = sorted(classVotes.iteritems(), key = lambda x: x[1], reverse = True)
    return sortedVotes[0][0]


def getAccuracy(testSet):
  correct = 0
  for x in range(len(testSet)):
    if testSet[x].predicted_type == testSet[x].fl_type:
      correct += 1
  return (correct/float(len(testSet))) * 100.0

def findResults(testSet, trainingSet, k):
  for one_test in testSet:
    with dataframe() as df:
      knns = df.add(knn, trainingSet, params = (one_test, k))
      one_test.predicted_type = getResponse(knns)

def main():
  trainingSet = []
  testSet = []
  split = 0.67
  loadDataset("iris.data", split, trainingSet, testSet)
  print 'Train set: ' + repr(len(trainingSet))
  print 'Test set: ' + repr(len(testSet))
  predictions = []
  k = 3
  THREADS = 4
  threads = []
  for i in range(THREADS):
    threads.append(Thread(target = findResults, args = (testSet[i::THREADS], trainingSet, k)))
    threads[-1].start()
  for i in range(THREADS):
    threads[i].join()
  accuracy = getAccuracy(testSet)
  print('Accuracy: ' + repr(accuracy) + '%')

main()
