#! /usr/bin/env python

import subprocess
import os
from decimal import Decimal
import datetime
import sys
import shutil
from math import log
import matplotlib.pyplot as plt
import operator

#Input path for the dataset 
path = sys.argv[1]

cmd = "find "+path+" -name '*.txt'" #Building the unix command to find the files ending with '.plt'
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(outputFiles, err) = p.communicate()

outputFolder = "transformed_tree_dataset_v2.1_1F/"
try:
  shutil.rmtree(outputFolder)
  os.stat(outputFolder)
except:
  os.mkdir(outputFolder)

for eachFile in outputFiles.strip().split('\n'):

  regionIndexEnd = eachFile.rfind('/') # test/36/
  pointIndex = eachFile.rfind('.') #test/36/008.
  regionIndexStart = eachFile[0:regionIndexEnd].rfind('/') # test/
  UserID = eachFile[regionIndexEnd+1:pointIndex]

  regionNo = eachFile[regionIndexStart+1:regionIndexEnd]
  '''try:
    os.stat(outputFolder+"/"+str(regionNo))
  except:
    os.mkdir(outputFolder+"/"+str(regionNo))'''

  #read each .txt file
  #inputPath = "/home/amar/project/Geo-Dataset/Output_Regions_old_116_20/sampletest.txt"
  inputPath = eachFile #10970/144.txt
  fileDescp = open(inputPath, 'r')
  newFileDescp = ''
  allData = fileDescp.read()

  #UserId = 165	
  lastTimeInterval = -1
  lastBS = -1
  lastTime = 0
  lastBestBS = -1
  timeLength = -1
  lastBoundary = -1
  timeInBS = {}
  finalDic = {}
  lastYear = -1
  lastMonth = -1
  lastDay = -1
  timeIntervalLength = 1 # In mins
  lastKey = ''
  lastValue = ''
  lastUserID = -1
  prevTime = 0 #used only when the new day is found, so the unsaved data till then will be persisted

  for eachline in allData.strip().split('\n'):
    #print eachFile
    tempList = [k.strip() for k in eachline.split(",") if k]
    UserId = int(tempList[0])
    year = int(tempList[1])
    month = int(tempList[2])
    day = int(tempList[3])
    hours = int(tempList[5])
    mins = int(tempList[6])
  
    currentBS = tempList[8]
    currentTime = (hours*60)+mins
    currentTimeInterval = int(currentTime/timeIntervalLength)
    currentBoundary = (currentTimeInterval+1)*timeIntervalLength

    # If the record is from the new day, then create records for the old day  
    if (year != lastYear or month != lastMonth or day != lastDay or lastUserID != UserId) and (lastYear != -1 and lastMonth != -1 and lastDay != -1 and lastUserID != -1):
      print "new day found-------------------------------->>"
      #print "before adding to timeInBS", timeInBS
      #print "lastBoundary=",lastBoundary
      #print "lastTime when new day is found..",lastTime
      #print prevTime
      timeLength = abs(prevTime -  lastTime) #time spent by last BS until the new day is found
      #print "timeLength===",timeLength
      if timeLength != 0:
        if lastBS in timeInBS: # add to the dict
          valE = int(timeInBS[lastBS])
          timeInBS[lastBS] = timeLength+valE
        else:
          timeInBS[lastBS] = timeLength
      #print "after adding to timeInBS", timeInBS
      #print "before adding to finalDic:",finalDic
      if len(timeInBS) > 0:
        lastBestBS = max(timeInBS.iteritems(), key=operator.itemgetter(1))[0] #get the BS where user has spent most time {'BS1':mins}
        finalDic[lastTimeInterval] = lastBestBS #add to the FINAL dict from where we generate the new records.
      #print "after adding to finalDic:",finalDic
      tempString = ''
      index = 0
      if len(finalDic) >= 2:
        for k in sorted(finalDic.keys()):
          lastKey = k
          lastValue = finalDic[k]
          if index < 2:
            tempString = tempString+str(k)+","+str(finalDic[k])+","
            index = index+1
          if index == 2:
            index = 1
            lenStr = len(tempString)
            #print "writing during change of day...."
            try:
    	      os.stat(outputFolder+"/"+str(regionNo))
            except:
              os.mkdir(outputFolder+"/"+str(regionNo))
            newFileDescp = open(outputFolder+regionNo+"/"+UserID+".txt",'a')
            newFileDescp.write(str(lastUserID)+","+str(day)+","+tempString[0:lenStr-1]+"\n")
            #print "wrote--------->", (str(lastUserID)+","+tempString[0:lenStr-1]+"\n")
            tempString = str(k)+","+str(finalDic[k])+","
      #Reset all the values so that each day will be treated separately
      lastTimeInterval = -1
      lastBS = -1
      lastTime = 0
      lastBestBS = -1
      timeLength = -1
      lastBoundary = -1
      timeInBS = {}
      finalDic = {}
      lastYear = -1
      lastMonth = -1
      lastDay = -1
      lastUserID = -1
  
    #same day, but the sample belongs to the different time interval
    if currentTimeInterval != lastTimeInterval and lastTimeInterval != -1:
      #somecode
      print "lastBoundary, lastTime,lastTimeInterval , currentTime, currentTimeInterval =",lastBoundary, lastTime, lastTimeInterval, currentTime, currentTimeInterval
      index = 0
      tempString = ''
      timeLength = abs(lastBoundary -  lastTime) #time spent by last BS until the new time interval
      if lastBS in timeInBS: # add to the dict
        valE = int(timeInBS[lastBS])
        timeInBS[lastBS] = timeLength+valE
      else:
        timeInBS[lastBS] = timeLength

      lastBestBS = max(timeInBS.iteritems(), key=operator.itemgetter(1))[0] #get the BS where user has spent most time {'BS1':mins}
      finalDic[lastTimeInterval] = lastBestBS #add to the FINAL dict from where we generate the new records.
      print "time interval change:", timeInBS
      print "final dic: ",finalDic
      timeInBS = {} #empty the dict which stores the {'BS1':mins} in a particular time interval

      noOfTimeIntervals = currentTimeInterval - lastTimeInterval
      if (noOfTimeIntervals) == 1:
        timeLength = abs(lastBoundary -  currentTime)#time spent by last BS(could be same as new BS) in the new time iterval
        if timeLength != 0:
          if lastBS in timeInBS:
            valE = int(timeInBS[lastBS])
            timeInBS[lastBS] = timeLength+valE
          else:
            timeInBS[lastBS] = timeLength
      print "new time interval vales for timeInBS:", timeInBS
      
      lastTime = currentTime
      lastBS = currentBS

      if len(finalDic) >= 2:
        for k in sorted(finalDic.keys()):
          lastKey = k
          lastValue = finalDic[k]
          if index < 2:
            tempString = tempString+str(k)+","+str(finalDic[k])+","
            index = index+1
          if index == 2:
            index = 1
            lenStr = len(tempString)
            print "Writing new records..."
            try:
    	      os.stat(outputFolder+"/"+str(regionNo))
            except:
              os.mkdir(outputFolder+"/"+str(regionNo))
            newFileDescp = open(outputFolder+regionNo+"/"+UserID+".txt",'a')
            newFileDescp.write(str(UserId)+","+str(day)+","+tempString[0:lenStr-1]+"\n")
            print "wrote--------->", (str(UserId)+","+tempString[0:lenStr-1]+"\n")
            tempString = str(k)+","+str(finalDic[k])+","
        finalDic = {}
        finalDic[lastKey] = lastValue
        print "after writing val of finalDic:",finalDic

    else: #input record belongs to the same day and same time interval
      if currentBS != lastBS and lastBS != -1: # BS has changed
        timeLength = abs(currentTime - lastTime)# time spent b/w 2 different bs
        if lastBS in timeInBS:# update or create time spent in a bs 
          valE = int(timeInBS[lastBS])
          timeInBS[lastBS] = timeLength+valE
        else:
          timeInBS[lastBS] = timeLength

      if currentBS != lastBS:
        lastBS = currentBS
        lastTime = currentTime 
  
    if currentTimeInterval != lastTimeInterval:
      lastTimeInterval = currentTimeInterval
    lastBoundary = currentBoundary
    lastYear = year
    lastMonth = month
    lastDay = day
    lastUserID = UserId
    prevTime = currentTime

  #print "Finally-----timeInBS:",  timeInBS
  #print "Finally --> FinDict",finalDic
  ##print "index::", index
  #print prevTime
  index = 0
  timeLength = abs(prevTime -  lastTime) #time spent by last BS until the new day is found
  #print "timeLength===",timeLength
  if timeLength != 0:
    if lastBS in timeInBS: # add to the dict
      valE = int(timeInBS[lastBS])
      timeInBS[lastBS] = timeLength+valE
    else:
      timeInBS[lastBS] = timeLength
  #print "after adding to timeInBS", timeInBS
  #print "before adding to finalDic:",finalDic
  if len(timeInBS) > 0:
    lastBestBS = max(timeInBS.iteritems(), key=operator.itemgetter(1))[0] #get the BS where user has spent most time {'BS1':mins}
    finalDic[lastTimeInterval] = lastBestBS #add to the FINAL dict from where we generate the new records.
    #print "after adding to finalDic:",finalDic
  tempString = ''
  if len(finalDic) >= 2:
    for k in sorted(finalDic.keys()):
      lastKey = k
      lastValue = finalDic[k]
      if index < 2:
        tempString = tempString+str(k)+","+str(finalDic[k])+","
        index = index+1
        if index == 2:
          index = 1
          lenStr = len(tempString)
          #print "writing record..................................."
          try:
    	    os.stat(outputFolder+"/"+str(regionNo))
          except:
            os.mkdir(outputFolder+"/"+str(regionNo))
          newFileDescp = open(outputFolder+regionNo+"/"+UserID+".txt",'a')
          newFileDescp.write(str(lastUserID)+","+str(day)+","+tempString[0:lenStr-1]+"\n")
          #print "wrote--------->", (str(lastUserID)+","+tempString[0:lenStr-1]+"\n")
          tempString = str(k)+","+str(finalDic[k])+","
