#! /usr/bin/env python

import subprocess
import os
from decimal import Decimal
import datetime
import sys
import shutil
from math import log

#Variables for minimum Latitude and Langitude
global minLat
#minLat = 91.000000
minLat = 500.000000
global minLang
#minLang = 181.000000
minLang = 500.000000

#Variables for maximum Latitude and Langitude
global maxLat
#maxLat = -91.000000
maxLat = 0.000000
global maxLang
#maxLang = -181.000000
maxLang = 0.000000

#No of vertical and horizontal Base stations (defining the ground for BS's)
divLong = 40000
divLati = 40000

maxLengthLong = int(log(divLong, 10) + 0.5)
maxLengthLat = int(log(divLati,10) + 0.5)
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


def checkValidLatLong(lat, lon):
  return ( lat >= -90 and lat <= 90 and lon >= -180 and lon <= 180) 

#find max & min latitude and langitude across the dataset
for eachFile in outputFiles.strip().split('\n'):
  
  fullPath = eachFile
  fullDS = open(fullPath, 'r')
  fullData = fullDS.read()

  afterfol = fullPath[0:fullPath.rfind('/')].rfind('/')
  beforefol = fullPath[0:afterfol].rfind('/')
  userID = fullPath[beforefol+1:afterfol]

  for eachline in fullData.strip().split('\n')[6:]: #for each line in each file stating from line #6
  
    tempList = [k.strip() for k in eachline.split(",") if k]

    localLat = float(tempList[0]) # Extract Latitude
    localLang = float(tempList[1])# Extract Langitude
    if not checkValidLatLong(localLat,localLang):
      continue

    if localLat < minLat:
      minLat = localLat
    if localLat > maxLat:
      maxLat = localLat

    if localLang < minLang:
      minLang = localLang
    if localLang > maxLang:
      maxLang = localLang
    
  fullDS.close()# end of innertempList for loop

#Compute differences in latitude & langitude
print "Max : Min - lati"
print maxLat,minLat

print "Max : Min - lang"
print maxLang,minLang

latiDiff = maxLat-minLat
longDiff = maxLang-minLang

print "lati , lang - diff"
print latiDiff,longDiff

#parameters to decide the Base station to be assigned for the user samples (Geo coordinates)
omegalat = latiDiff/divLati
omegalang = longDiff/divLong

print "omegalat:",omegalat
print "omegalang", omegalang

prevUserID = -1
outFolder = 'Output3/'

try:
    shutil.rmtree(outFolder)
    os.stat(outFolder)
except:
    os.mkdir(outFolder)  
#Create the dataset in the required format
for eachFile in outputFiles.strip().split('\n'):
  
  
  fullPath = eachFile
  fileDS = open(fullPath, 'r')
  #fullData = fileDS.read()
  
  afterfol = fullPath[0:fullPath.rfind('/')].rfind('/')
  beforefol = fullPath[0:afterfol].rfind('/')
  userID = fullPath[beforefol+1:afterfol]
  #print userID
  
  #for the 1st file, anyways create a file with the userID (name)
  if(prevUserID == -1):
    prevUserID = userID
    finalEntries = open(outFolder+userID+".txt",'w')
  #For every other file, create new folder only if user changes
  if(userID != prevUserID):
    finalEntries.close()
    finalEntries = open(outFolder+userID+".txt",'w')
    prevUserID = userID

  formatStr = "%." + str(maxLengthLat) + "d%." + str(maxLengthLong) + "d" 
  #print formatStr
  
  cnt = 0
  for eachline in fileDS:
    cnt = cnt+1   
    
    if cnt <= 6:
      continue

    tempList = [k.strip() for k in eachline.split(",") if k]

    localLat = float(tempList[0])
    localLang = float(tempList[1])
    if not checkValidLatLong(localLat,localLang):
      continue
    localrelLat = localLat - minLat
    localrelLang = localLang - minLang

    bsp1 = int(localrelLat / omegalat)
    bsp2 = int(localrelLang / omegalang)
    
    newbsp1 = bsp1*divLati
    newbsp2 = bsp2
    finalBSVal = newbsp1 +newbsp2
     
    #temp = formatStr % (bsp1, bsp2)
    temp = str(finalBSVal)

    mdtf = datetime.datetime.strptime(str(tempList[5])+" "+str(tempList[6]), '%Y-%m-%d %H:%M:%S')
    #entry = str(userID)+","+str(mdtf.year)+","+str(mdtf.month)+","+str(mdtf.day)+","+str(dictDays[mdtf.weekday()])+","+str(mdtf.hour)+","+str(mdtf.minute)+","+str(mdtf.second)+",B"+ temp
    entry = str(userID)+","+str(mdtf.year)+","+str(mdtf.month)+","+str(mdtf.day)+","+str(mdtf.weekday())+","+str(mdtf.hour)+","+str(mdtf.minute)+","+str(mdtf.second)+","+ temp
    finalEntries.write(entry+"\n")
  fileDS.close()#end of outer for loop

#End of program

