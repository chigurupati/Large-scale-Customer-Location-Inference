#! /usr/bin/env python

import numpy as np
#from pylab import *
import pylab as P
import subprocess
import os
from decimal import Decimal
import datetime
import sys
import shutil
from math import log
import matplotlib.pyplot as plt
import collections
from operator import itemgetter

#Input path for the dataset 
path = sys.argv[1]

cmd = "find "+path+" -name '*.txt'" #Building the unix command to find the files ending with '.plt'
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(outputFiles, err) = p.communicate()
dictTemp1 ={}
usersPerBS = []
#read each .txt file

  #noOfUsers = 0.0
for eachFile in outputFiles.strip().split('\n'):
  #print eachFile
  fullPath = eachFile
  fullDS = open(fullPath, 'r')
  fullData = fullDS.read()
  
  afterfol = fullPath.rfind('.')
  beforefol = fullPath.rfind('/')
  userID = fullPath[beforefol+1:afterfol]
    #print userID
  listBS = []
  for eachline in fullData.strip().split('\n'): #for each line in each file stating from line #6
    #isFound = False;
    tempList = [k.strip() for k in eachline.split(",") if k]
    listBS.append(tempList[8])
    #print "tempList[8]",int(tempList[8])
    #if int(i) == int(tempList[8]):
      #noOfUsers = noOfUsers+1.0
      #break;
  setBS = set(listBS)
  for eachBS in setBS:
    if eachBS in dictTemp1.keys():
      tempVal = dictTemp1[eachBS]
      dictTemp1[eachBS] = tempVal+1.0
    else:
      dictTemp1[eachBS] = 1
print dictTemp1

for r in dictTemp1.keys():
  if dictTemp1[r] >= 1000:
    print r
for each1 in dictTemp1.values():
  if each1 >= 1000:
    #print eachRegion
    sys.exit("higher number of users")
  else:
    usersPerBS.append(each1)
#usersPerBS.append(noOfUsers)
#print usersPerBS
#Drawing CDF - number of users for each BS
counter=collections.Counter(sorted(usersPerBS))
counter = sorted(counter.items(), key=itemgetter(0))
print counter
xVal = []
yVal = []
for o in counter:
  xVal.append(float(o[0]))
  yVal.append(float(o[1]))
#print counter
X = xVal
Y = yVal
fac = float(len(usersPerBS))
Y[:] = [x / fac for x in Y ]
CY = np.cumsum(Y)

P.xlabel('No. of User ( #Users )')
P.ylabel('Probability of BS')
P.plot(X,CY,'r--')
P.suptitle('Distribution of #BS to the Users  ')
P.figure()
#P.show()

#-------------------------------------
dictTemp ={}

for eachFile in outputFiles.strip().split('\n'):
  #print eachFile
  fullPath = eachFile
  fullDS = open(fullPath, 'r')
  fullData = fullDS.read()
  
  afterfol = fullPath.rfind('.')
  beforefol = fullPath.rfind('/')
  userID = fullPath[beforefol+1:afterfol]
  print userID
   
  listBS = []
  for eachline in fullData.strip().split('\n'): #for each line in each file stating from line #6
    tempList = [k.strip() for k in eachline.split(",") if k]
    listBS.append(float(tempList[8]))
  setBS = set(listBS)
  noOfBS = float(len(setBS))
  print "no of BS",noOfBS
  #break
  #bsPerUser.append(noOfBS)
  dictTemp[userID] = noOfBS

#print dictTemp
#print sorted(dictTemp.values())

print (dictTemp)
#Drawing CDF - no of BS per user
counter = collections.Counter(sorted(dictTemp.values()))
print "Drawing CDF - no of BS per user"
print counter
counter = sorted(counter.items(), key=itemgetter(0))
print counter
xVal = []
yVal = []
for o in counter:
  xVal.append(float(o[0]))
  yVal.append(float(o[1]))
#print counter
X = xVal
Y = yVal
fac = float(len(dictTemp.values()))
Y[:] = [x / fac for x in Y ]
#print Y
CY = np.cumsum(Y)
#print "cy", CY

P.xlabel('No. of Base stations ( #BS )')
P.ylabel('Probability of Users')
P.plot(X,CY,'b--')
P.suptitle('Distribution of #Users to BS')
#P.figure()
P.show()
