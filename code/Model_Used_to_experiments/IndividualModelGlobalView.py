from pyspark import SparkConf, SparkContext
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from os import walk, path
import time


#logFile = "/home/amar/Desktop/Material/Course/Project_sem/Geo-Dataset/train-data-t/2017/"
#logData = sc.textFile(logFile).filter(lambda s: s != "")
#data = logData.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:7]))
##lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],LabeledPoint([k.strip() for k in x.split(",") if k][0:8])
#(trainingData, testData) = data.randomSplit([0.8, 0.2])

sc = SparkContext("local", "Individual model")
trainFile = "/home/amar/project/Geo-Dataset/Individual_User_Dataset/trainData"
testFile = "/home/amar/project/Geo-Dataset/Individual_User_Dataset/testData"
ModelDict = {}
errorPredictions = open("Individual_User_Dataset_global_view.log",'a')
errorPredictionsTree = open("Individual_User_Dataset_global_view_trees.log",'a')
avg = 0.0
count = 0.0
#For training and building models
for root, dirs, files in walk(trainFile, topdown=True):
  for name in files:
    start_time = time.time()

    trainRegion = path.join(root, name)    
    testRegion = path.join(testFile, name)
    #print trainRegion,testRegion
    count = count+1.0

    trainRDD = sc.textFile(trainRegion)
    testRDD = sc.textFile(testRegion)
    
    #trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][7])+[k.strip() for k in x.split(",") if k][2:7]))
    #testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][7])+[k.strip() for k in x.split(",") if k][2:7]))

    #trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:8]))
    #testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:8]))

    # new transformed dataset:  nxtLoc <-- Day+ CurrTimeInt+ CurrLoc+ NxtTimeInt
    #trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][1:5]))
    #testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][1:5]))
   
    # new transformed dataset:  nxtLoc <-- Day+ CurrLoc+ NxtTimeInt
    trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][1:2])+[k.strip() for k in x.split(",") if k][3:5]))
    testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][1:2])+[k.strip() for k in x.split(",") if k][3:5]))

    model = DecisionTree.trainClassifier(trainingData, numClasses=88000, categoricalFeaturesInfo={}, impurity='entropy', maxDepth=10, maxBins=32)
    #ModelDict[name] = model

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    avg = avg+testErr
    print('Test Error = ' + str(testErr))
    print('Learned classification tree model:')
    print(model.toDebugString())
    errorPredictions.write("User: "+str(name)+" Test_Error=: "+str(testErr)+" TimeTaken: "+str((time.time() - start_time))+" trainRDD_count: "+str(trainRDD.count())+" testRDD_count: "+ str(testRDD.count())+'\n')
    errorPredictionsTree.write("UserID: "+str(name)+"  Test Error rate: "+str(testErr)+'\n'+"Model:"+"\n"+model.toDebugString())

print "Average error rate: "+str(avg/count)
errorPredictions.write("Average_error_rate: "+str(avg/count))
errorPredictionsTree.write("Average error rate: "+str(avg/count))
errorPredictions.close()
errorPredictionsTree.close()
