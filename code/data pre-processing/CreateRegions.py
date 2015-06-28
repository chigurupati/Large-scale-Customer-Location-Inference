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
minLat = 91.000000
#minLat = 500.000000
global minLong
minLong = 181.000000
#minLong = 500.000000

#Variables for maximum Latitude and Langitude
global maxLat
maxLat = -91.000000
#maxLat = 0.000000
global maxLong
maxLong = -181.000000
#maxLong = 0.000000

numRowRegion = 100
numColumnRegion = 100

#No of vertical and horizontal Base stations in (defining the ground for BS's)
noBSRow = 20
noBSColumn = 20

quarterDict = {1:1,2:1,3:1,4:2,5:2,6:2,7:3,8:3,9:3,10:4,11:4,12:4}

maxLengthLong = int(log(noBSColumn, 10) + 0.5)
maxLengthLat = int(log(noBSRow,10) + 0.5)

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
    localLong = float(tempList[1])# Extract Langitude
    if not checkValidLatLong(localLat,localLong):
      continue

    if localLat < minLat:
      minLat = localLat
    if localLat > maxLat:
      maxLat = localLat

    if localLong < minLong:
      minLong = localLong
    if localLong > maxLong:
      maxLong = localLong
    
  fullDS.close()# end of innertempList for loop

#Compute differences in latitude & langitude
print "Max : Min - lati"
print maxLat,minLat

print "Max : Min - lang"
print maxLong,minLong

latiDiff = maxLat-minLat
longDiff = maxLong-minLong

print "lati , lang - diff"
print latiDiff,longDiff

#dividing the distance to longitude based on required no of rows
rowSizeRegion = (longDiff/numRowRegion) 
columnSizeRegion = (latiDiff / numColumnRegion)

print "Size of Row and Column for regions: with no of regions as",numRowRegion 
print rowSizeRegion,columnSizeRegion

#print "no of sub region columns and rows:",noBSRow,noBSColumn

prevUserID = -1
outFolder = 'Output_Regions/'

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
  ##print userID
  '''
  #for the 1st file, anyways create a file with the userID (name)
  if(prevUserID == -1):
    prevUserID = userID
    finalEntries = open(outFolder+userID+".txt",'w')
  #For every other file, create new folder only if user changes
  if(userID != prevUserID):
    finalEntries.close()
    finalEntries = open(outFolder+userID+".txt",'w')
    prevUserID = userID '''

  formatStr = "%." + str(maxLengthLat) + "d%." + str(maxLengthLong) + "d" 
  ##print formatStr
  
  cnt = 0
  for eachline in fileDS:
    cnt = cnt+1   
    
    if cnt <= 6:
      continue

    tempList = [k.strip() for k in eachline.split(",") if k]

    localLat = float(tempList[0])
    localLong = float(tempList[1])
    #print "actual - localLat,localLong : ",localLat,localLong

    if not checkValidLatLong(localLat,localLong):
      continue

    omegaRegionLat = int((localLat-minLat)/columnSizeRegion)
    omegaRegionLong = int((localLong-minLong)/rowSizeRegion)
    
    #print "omegaRegionLat,omegaRegionLong: Region-index",omegaRegionLat,omegaRegionLong   
    #from the index 
    subRegionIndex = omegaRegionLat+(numColumnRegion*omegaRegionLong)
    #print "Region Index",subRegionIndex

    try:
      #shutil.rmtree(outFolder)
      os.stat(outFolder+"/"+str(subRegionIndex))
    except:
      os.mkdir(outFolder+"/"+str(subRegionIndex))

    '''try:
      #os.stat(outFolder+"/"+str(subRegionIndex))
      os.stat(outFolder+"/"+str(subRegionIndex)+"/"+str(userID))
    except:
      #os.mkdir(outFolder+"/"+str(subRegionIndex))
      os.mkdir(outFolder+"/"+str(subRegionIndex)+"/"+str(userID))'''

    #writeToFile = open(outFolder+str(subRegionIndex)+"/"+str(userID)+"/"+str(userID)+".txt",'wa')    
    writeToFile = open(outFolder+str(subRegionIndex)+"/"+str(userID)+".txt",'a')

    rowSizeBS = rowSizeRegion / noBSRow
    columnSizeBS = columnSizeRegion / noBSColumn
  
    localSRLat = localLat - ((omegaRegionLat*columnSizeRegion) +minLat) # local lati and long in the sub region 
    localSRLong = localLong - ((omegaRegionLong*rowSizeRegion) + minLong)
    #print "localSRLat,localSRLong - local to sub region",localSRLat,localSRLong

    omegaBSLat = int(localSRLat/columnSizeBS) # finding which index in the sub region (inner region)
    omegaBSLong = int(localSRLong/rowSizeBS)
    #print "omegaBSLat,omegaBSLong - sub Region index:",omegaBSLat,omegaBSLong

    InnerSubRegIndex = omegaBSLat+(noBSColumn*omegaBSLong)
    #print "InnerSubRegIndex (BS):",InnerSubRegIndex

    #temp = formatStr % (bsp1, bsp2)
   
    
    
    #print "userID",userID,InnerSubRegIndex,"\n"
 
    mdtf = datetime.datetime.strptime(str(tempList[5])+" "+str(tempList[6]), '%Y-%m-%d %H:%M:%S')
    #entry = str(userID)+","+str(mdtf.year)+","+str(mdtf.month)+","+str(mdtf.day)+","+str(dictDays[mdtf.weekday()])+","+str(mdtf.hour)+","+str(mdtf.minute)+","+str(mdtf.second)+",B"+ temp
    entry = str(userID)+","+str(mdtf.year)+","+str(mdtf.month)+","+str(mdtf.day)+","+str(mdtf.weekday())+","+str(mdtf.hour)+","+str(mdtf.minute)+","+str(quarterDict[mdtf.month])+","+ str(InnerSubRegIndex)
    writeToFile.write(entry+"\n")
    writeToFile.close()#end of inner for loop

#End of program

