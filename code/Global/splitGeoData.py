#! /usr/bin/env python

import subprocess
import os
from decimal import Decimal
import datetime
import sys
import shutil
from math import log


#Input path for the dataset 
path = sys.argv[1]

cmd = "find "+path+" -name '*.txt'" #Building the unix command to find the files ending with '.plt'
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(outputFiles, err) = p.communicate()

outputFolder = "dataset_2.0/"
try:
  shutil.rmtree(outputFolder)
  os.stat(outputFolder)
except:
  os.mkdir(outputFolder)

trainingFol = "dataset_2.0/trainData/"
testFol = "dataset_2.0/testData/"

os.mkdir(trainingFol)
os.mkdir(testFol)


#read each .txt file
for eachFile in outputFiles.strip().split('\n'):
  
  fullPath = eachFile
  fullDS = open(fullPath, 'r')
  fullData = fullDS.read().strip()
 
  tempList = [k.strip() for k in fullData.split("\n") if k]
	#test/36/008.txt
  regionIndexEnd = fullPath.rfind('/') # test/36/
  pointIndex = fullPath.rfind('.') #test/36/008.
  regionIndexStart = fullPath[0:regionIndexEnd].rfind('/') # test/

  regionNo = fullPath[regionIndexStart+1:regionIndexEnd]
  UserID = fullPath[regionIndexEnd+1:pointIndex]
  noOfRecords = len(tempList)
 
  percent20 =  (20*noOfRecords)/100
  percent80 =  (80*noOfRecords)/100

  try:
    os.stat(trainingFol+"/"+str(regionNo))
  except:
    os.mkdir(trainingFol+"/"+str(regionNo))

  try:
    os.stat(testFol+"/"+str(regionNo))
  except:
    os.mkdir(testFol+"/"+str(regionNo))

  percent80FD = open(trainingFol+regionNo+"/"+UserID+".txt",'a')
  percent20FD = open(testFol+regionNo+"/"+UserID+".txt",'a')
  
  
  percent20FD.write('\n'.join(tempList[percent80:]))
  percent80FD.write('\n'.join(tempList[0:percent80]))
  percent20FD.close()
  percent80FD.close()
  print regionNo,UserID,noOfRecords,percent20,percent80

