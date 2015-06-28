from pyspark import SparkConf, SparkContext
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from os import walk, path
import time
from pyspark.mllib.tree import RandomForest

#logFile = "/home/amar/Desktop/Material/Course/Project_sem/Geo-Dataset/train-data-t/2017/"
#logData = sc.textFile(logFile).filter(lambda s: s != "")
#data = logData.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:7]))
##lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],LabeledPoint([k.strip() for k in x.split(",") if k][0:8])
#(trainingData, testData) = data.randomSplit([0.8, 0.2])

sc = SparkContext("local", "global model")
trainFile = "/home/amar/project/Geo-Dataset/dataset_TTDS_v2.1_1F_more100SAM/trainData/*/"
testFile = "/home/amar/project/Geo-Dataset/dataset_TTDS_v2.1_1F_more100SAM/testData/*/"
ModelDict = {}
errorPredictions = open("model_errors_global_area_RF_TTDS_v2.1_1F_more100SAM.txt",'a')
errorPredictionsTree = open("model_errorsTree_Global_area_RF_TTDS_v2.1_1F_more100SAM.txt",'a')

start_time = time.time()
##trainRegion = path.join(root, name)
#testRegion = path.join(testFile, name)

trainRDD = sc.textFile(trainFile)
testRDD = sc.textFile(testFile)
    
#trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][7])+[k.strip() for k in x.split(",") if k][2:7]))
#testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][7])+[k.strip() for k in x.split(",") if k][2:7]))

#trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:8]))
#testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:8]))
   

#trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][1:4]))
#testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][1:4]))

# new transformed dataset:  nxtLoc <-- Day+ CurrLoc+ NxtTimeInt
trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][1:2])+[k.strip() for k in x.split(",") if k][3:5]))
testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],list([k.strip() for k in x.split(",") if k][1:2])+[k.strip() for k in x.split(",") if k][3:5]))

model = RandomForest.trainClassifier(trainingData, numClasses=400, categoricalFeaturesInfo={},numTrees=10, featureSubsetStrategy="auto", impurity='entropy', maxDepth=6, maxBins=32)
  
# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())

print('Test Error = ' + str(testErr))
print('Learned classification tree model:')
print(model.toDebugString())
errorPredictions.write("Test Error = : "+str(testErr)+" Time taken:"+str((time.time() - start_time))+" trainRDD count:"+str(trainRDD.count())+" testRDD count: "+ str(testRDD.count())+'\n')
errorPredictionsTree.write("  Test Error rate: "+str(testErr)+'\n'+"Model:"+"\n"+model.toDebugString())

errorPredictions.close()
errorPredictionsTree.close()
