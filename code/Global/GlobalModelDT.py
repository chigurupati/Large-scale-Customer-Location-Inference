from pyspark import SparkConf, SparkContext
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint

#import pyspark

#please put the correct path below
logFile = '/media/amar/OS/My_things/Course/Project_sem/Large-scale-Customer-Location-Inference/code/Global/Sample_small_input'

sc = SparkContext("local", "global model")
logData = sc.textFile(logFile).filter(lambda s: s != "")
data = logData.map(lambda x: LabeledPoint([k.strip() for k in x.split(",") if k][-1],[k.strip() for k in x.split(",") if k][0:8]))


(trainingData, testData) = data.randomSplit([0.8, 0.2])

'''print "Total Data count:"
print logData.count()

print "Taining data: \n"
print trainingData.count()

print "test data:"
print testData.count()'''


model = DecisionTree.trainClassifier(trainingData, numClasses=10001, categoricalFeaturesInfo={}, impurity='gini', maxDepth=15, maxBins=32)

#model = DecisionTree.train(trainingData,'Classification',impurity='gini',numClasses=2500000000, maxDepth=15,maxBins=100)

#model = DecisionTree.trainClassifier(trainingData, numClasses=4000040000, categoricalFeaturesInfo={}, impurity='entropy', maxDepth=15, maxBins=100)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
print('Test Error = ' + str(testErr))
print('Learned classification tree model:')
print(model.toDebugString())
