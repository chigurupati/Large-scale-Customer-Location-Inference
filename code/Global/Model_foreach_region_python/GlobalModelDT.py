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

sc = SparkContext("local", "global model")
trainFile = "/home/amar/Desktop/Material/Course/Project_sem/Geo-Dataset/dataset_2.0/trainData"
testFile = "/home/amar/Desktop/Material/Course/Project_sem/Geo-Dataset/dataset_2.0/testData"
ModelDict = {}
errorPredictions = open("model_errors.txt",'a')
errorPredictionsTree = open("model_errorsTree.txt",'a')
avg = 0.0
#For training and building models
for root, dirs, files in walk(trainFile, topdown=True):
  for name in dirs:
    
    start_time = time.time()
    trainRegion = path.join(root, name)
    testRegion = path.join(testFile, name)

    trainRDD = sc.textFile(trainRegion)
    testRDD = sc.textFile(testRegion)

    trainingData = trainRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:7]))
    testData = testRDD.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][2:7]))

    model = DecisionTree.trainClassifier(trainingData, numClasses=40000, categoricalFeaturesInfo={}, impurity='entropy', maxDepth=10, maxBins=32)
    #ModelDict[name] = model

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    avg = avg+testErr
    print('Test Error = ' + str(testErr))
    print('Learned classification tree model:')
    print(model.toDebugString())
    errorPredictions.write("Region ID: "+str(name)+"   Test Error = : "+str(testErr)+" Time taken:"+str((time.time() - start_time))+'\n')
    errorPredictionsTree.write("Region ID: "+str(name)+"  Test Error rate: "+str(testErr)+'\n'+"Model:"+"\n"+model.toDebugString())

print "Average error rate: "+str(avg/20.0)
errorPredictions.write("Average error rate: "+str(avg/20.0))
errorPredictionsTree.write("Average error rate: "+str(avg/20.0))
errorPredictions.close()
errorPredictionsTree.close()
