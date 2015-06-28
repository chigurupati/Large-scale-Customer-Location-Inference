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

cmd = "find "+path+" -name '*.txt'" #Building the unix command to find the files ending with '.txt'
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(outputFiles, err) = p.communicate()

print outputFiles

bsPerUser = []
for i in range(0,182):
  noOfBS = 0.0
  for eachFile in outputFiles.strip().split('\n'):
    ##print eachFile
    fullPath = eachFile
    afterfol = fullPath.rfind('.')
    beforefol = fullPath.rfind('/')
    userID = fullPath[beforefol+1:afterfol]
    ##print userID
    if int(i) != int(userID):
      continue;

    fullDS = open(fullPath, 'r')
    fullData = fullDS.read()

    listBS = []
    for eachline in fullData.strip().split('\n'):
      tempList = [k.strip() for k in eachline.split(",") if k]
      listBS.append(float(tempList[8]))
    setBS = set(listBS)
    
    noOfBSTemp = float(len(setBS))
    noOfBS = noOfBS + noOfBSTemp
    print noOfBS
    #break
  bsPerUser.append(noOfBS)

#Drawing CDF - no of BS per user
counter=collections.Counter(sorted(bsPerUser))
counter = sorted(counter.items(), key=itemgetter(0))
print counter
xVal = []
yVal = []
for o in counter:
  xVal.append(float(o[0]))
  yVal.append(float(o[1]))
##print counter
X = xVal
Y = yVal
print "X",X
print "Y initial",Y
fac = len(bsPerUser)
Y[:] = [xi / fac for xi in Y ]
print "Y",Y
CY = np.cumsum(Y)
print "cy", CY

P.xlabel('No. of Base stations ( #BS )')
P.ylabel('Probability of Users')
P.plot(X,CY,'k--')
P.suptitle('Distribution of #Users to BS across the entire area (whole area..)')
P.figure()
#P.show()

#------------------------
#find -path './Output_Regions_old_116200_small/*' -prune -type d
usersPerBS = []
cmd = "find -path '"+path+"*' -prune -type d" #Building the unix command to find the directories
#print cmd
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(outputRegions, err) = p.communicate()
print outputRegions
noOfUsers1 = []
for eachRegion in outputRegions.strip().split('\n'):
  cmdUsers = "find "+eachRegion+" -name '*.txt'"
  p1 = subprocess.Popen(cmdUsers, stdout=subprocess.PIPE, shell=True)
  (outputUsersFile, err) = p1.communicate()
  #for BSID in range(1,401):
  noOfUsers = 0.0
  dictTemp = {}
  for eachFile in outputUsersFile.strip().split('\n'):
    fullPath = eachFile
    fullDS = open(fullPath, 'r')
    fullData = fullDS.read()

    listBS = []
    for eachline in fullData.strip().split('\n'): #for each line in each file stating from line #6
  
      tempList = [k.strip() for k in eachline.split(",") if k]
      listBS.append(tempList[8])

    setBS = set(listBS)

    for eachBS in setBS:
      if eachBS in dictTemp.keys():
        tempVal = dictTemp[eachBS]
        dictTemp[eachBS] = tempVal+1.0
      else:
        dictTemp[eachBS] = 1
  print "no of users in each BS",dictTemp
  for each1 in dictTemp.values():
    if each1 >= 1790:
      print eachRegion
      sys.exit("higher number of users")
    else:
      noOfUsers1.append(each1)
#print "noOfUsers1",noOfUsers1

#Drawing CDF - number of users for each BS
counter=collections.Counter(sorted(noOfUsers1))
counter = sorted(counter.items(), key=itemgetter(0))
print counter
xVal = []
yVal = []
for o in counter:
  xVal.append(float(o[0]))
  yVal.append(float(o[1]))
##print counter
X = xVal
Y = yVal
#print "counter",counter

#X = counter.keys()
#Y = counter.values()
print "X",X
print "Y",Y
fac = float(len(noOfUsers1))
Y[:] = [xi / fac for xi in Y ]

CY = np.cumsum(Y)
print "Cy",CY
P.xlabel('No. of User ( #Users )')
P.ylabel('Probability of BS')
P.plot(X,CY,'r--')
P.suptitle('Distribution of #BS to the Users across the entire area (whole area..) ')
P.show()
