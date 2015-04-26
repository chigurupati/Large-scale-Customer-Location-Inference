#! /usr/bin/env python

import subprocess
import os
from decimal import Decimal
import datetime
import sys

#Variables for minimum Latitude and Langitude
global minLat
minLat = 500.000000
global minLang
minLang = 500.000000

#Variables for maximum Latitude and Langitude
global maxLat
maxLat = 0.000000
global maxLang
maxLang = 0.000000

#No of vertical and horizontal Base stations (defining the ground for BS's)
divLong = 15.000000
divLati = 15.000000

#Format for representing Base stations
expBSLen = 2
appZero = "0"

#Dictionary of weekdays 
dictDays = {0:'Mon',1:'Tue',2:'Wed',3:'Thu',4:'Fri',5:'Sat',6:'Sun'}

#Input path for the dataset 
path = sys.argv[1]

cmd = "find "+path+" -name '*.plt'" #Building the unix command to find the files ending with '.plt'
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(outputFiles, err) = p.communicate()

#find max & min latitude and langitude across the dataset
for eachFile in outputFiles.strip().split('\n'):
  
  fullPath = eachFile
  fullDS = open(fullPath, 'r')
  fullData = fullDS.read()

  for eachline in fullData.strip().split('\n')[6:]: #for each line in each file stating from line #6
   
    tempList = [k.strip() for k in eachline.split(",") if k]

    localLat = float(tempList[0]) # Extract Latitude
    localLang = float(tempList[1])# Extract Langitude

    if localLat < minLat:
      minLat = localLat
    if localLang < minLang:
      minLang = localLang
    if localLat > maxLat:
      maxLat = localLat
    if localLang > maxLang:
      maxLang = localLang

  fullDS.close()# end of inner for loop

#Compute differences in latitude & langitude
latiDiff = maxLat-minLat
longDiff = maxLang-minLang

#parameters to decide the Base station to be assigned for the user samples (Geo coordinates)
omegalat = latiDiff/divLati
omegalang = longDiff/divLong

#Create the dataset in the required format
finalEntries = open("dataset-geo.txt",'w')
for eachFile in outputFiles.strip().split('\n'):

  fullPath = eachFile
  fileDS = open(fullPath, 'r')
  fullData = fileDS.read()

  afterfol = fullPath[0:fullPath.rfind('/')].rfind('/')
  beforefol = fullPath[0:afterfol].rfind('/')
  userID = fullPath[beforefol+1:afterfol]
  print userID

  for eachline in fullData.strip().split('\n')[6:]: #for each line in each file stating from line #6
    #print eachline
    tempList = [k.strip() for k in eachline.split(",") if k]

    localLat = float(tempList[0])
    localLang = float(tempList[1])
    localrelLat = localLat - minLat
    localrelLang = localLang - minLang
    bsp1 = int(localrelLat / omegalat)
    bsp2 = int(localrelLang / omegalang)
    bsp1str = ""
    bsp2str = ""
    if len(str(bsp1)) != expBSLen:
      bsp1str = appZero+str(bsp1)
    else:
      bsp1str = str(bsp1)
    if len(str(bsp2)) != expBSLen:
      bsp2str = appZero+str(bsp2)
    else:
      bsp2str = str(bsp2)

    #print "part1-",localrelLat / omegalat
    #print "part2-",localrelLang / omegalang

    #print "part1-",bsp1str
    #print "part2-",bsp2str
    mdtf = datetime.datetime.strptime(str(tempList[5])+" "+str(tempList[6]), '%Y-%m-%d %H:%M:%S')
    entry = str(userID)+","+str(mdtf.year)+","+str(mdtf.month)+","+str(mdtf.day)+","+str(dictDays[mdtf.weekday()])+","+str(mdtf.hour)+","+str(mdtf.minute)+",B"+bsp1str+bsp2str
    finalEntries.write(entry+"\n")
  fileDS.close()#end of outer for loop
finalEntries.close()

#End of program
