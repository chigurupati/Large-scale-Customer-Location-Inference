#! /usr/bin/env python
import subprocess
import os
import sys


#Input path for the dataset 
path = sys.argv[1]

cmd = "find "+path+" -name '*.txt'" #Building the unix command to find the files ending with '.plt'
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
(outputFiles, err) = p.communicate()

#read each .txt file

outputFolder = "Individual_User_Dataset/"
try:
  #shutil.rmtree(outputFolder)
  os.stat(outputFolder)
except:
  os.mkdir(outputFolder)

#subFolder = "Individual_User_Dataset/trainData/"
subFolder = "Individual_User_Dataset/testData/"

os.mkdir(subFolder)
#another comment

encodingsDic = {}
counter = 1
encodedVal = 0
for eachFile in outputFiles.strip().split('\n'):
  #print eachFile
  fullPath = eachFile
  fullDS = open(fullPath, 'r')
  fullData = fullDS.read()

  
  print("hello")
  regionIndexEnd = eachFile.rfind('/') # test/36/
  regionIndexStart = eachFile[0:regionIndexEnd].rfind('/') # test/
  regionNo = eachFile[regionIndexStart+1:regionIndexEnd]
  #Just another commad by blue2 branch
  afterfol = fullPath.rfind('.')
  beforefol = fullPath.rfind('/')
  userID = fullPath[beforefol+1:afterfol]
  
  #print regionNo, userID
  
  if regionNo in encodingsDic: # add to the dict
    encodedVal = int(encodingsDic[regionNo])
  else:
    encodingsDic[regionNo] = counter
    encodedVal = counter
    counter = counter+1

  for eachline in fullData.strip().split('\n'): #for each line in each file stating from line #6
    fileDescriptor = open(subFolder+userID+".txt",'a')
    #isFound = False;
    tempList = [k.strip() for k in eachline.split(",") if k]
    day = tempList[1]
    currTimeInter = tempList[2]
    currLoc = str(encodedVal)+tempList[3]
    nxtTimeInter = tempList[4]
    nextLoc = str(encodedVal)+tempList[5]
    fileDescriptor.write(userID+","+day+","+currTimeInter+","+currLoc+","+nxtTimeInter+","+nextLoc+"\n")

  fileDescriptor.close()

