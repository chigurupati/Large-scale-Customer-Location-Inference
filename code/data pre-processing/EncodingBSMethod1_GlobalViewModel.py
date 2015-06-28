#! /usr/bin/env python
import subprocess
import os
import sys
from os import walk, path

#Input path for the dataset 
path = sys.argv[1]

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
encodingsDic = {}
counter = 0
encodedVal = 0
NoofBSInRegion = 400 #No of BS considered in the region

#For training and building models
for root, dirs, files in walk(path, topdown=True):
  for name in dirs:

    #eachRegion = path.join(root, name)
    eachRegion = path+name+"/"
    print eachRegion
    cmd = "find "+eachRegion+" -name '*.txt'" #Building the unix command to find the files ending with '.txt'
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    (outputFiles, err) = p.communicate()
    #print outputFiles
    
    for eachFile in outputFiles.strip().split('\n'):
      print eachFile
      fullPath = eachFile
      fullDS = open(fullPath, 'r')
      fullData = fullDS.read()

      regionIndexEnd = eachFile.rfind('/') # test/36/
      regionIndexStart = eachFile[0:regionIndexEnd].rfind('/') # test/
      regionNo = eachFile[regionIndexStart+1:regionIndexEnd]
  
      afterfol = fullPath.rfind('.')
      beforefol = fullPath.rfind('/')
      userID = fullPath[beforefol+1:afterfol]

      for eachline in fullData.strip().split('\n'): #for each line in each file stating from line #6
        fileDescriptor = open(subFolder+userID+".txt",'a')
        #isFound = False;
        tempList = [k.strip() for k in eachline.split(",") if k]
        day = tempList[1]
        currTimeInter = tempList[2]
        currLoc = counter*NoofBSInRegion+int(tempList[3])
        #print "counter= ", counter
        #print "Curr loc=",tempList[3],currLoc
        nxtTimeInter = tempList[4]
        nextLoc = counter*NoofBSInRegion+int(tempList[5])
        #print "next loc =",tempList[5],nextLoc
        fileDescriptor.write(userID+","+day+","+currTimeInter+","+str(currLoc)+","+nxtTimeInter+","+str(nextLoc)+"\n")

        fileDescriptor.close()
    counter = counter+1
