import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurity
//import scala.collection.JavaConverters._	

object IndividualRegionModel {
  def main(args: Array[String]) {
    val trainFile = "/home/amar/Desktop/Material/Course/Project_sem/Geo-Dataset/dataset_2.0/trainData/2024/"
    val testFile = "/home/amar/Desktop/Material/Course/Project_sem/Geo-Dataset/dataset_2.0/testData/2024/"
    //val logFile = "/home/amar/Desktop/Material/Course/Project_sem/Geo-Dataset/Output3/2024/"

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    /*val logData = sc.textFile(logFile)
    
    val data = logData.map{ line =>
	val values = line.split(',').map(_.toDouble)
        var l = values.init
    	val featureVec = Vectors.dense( Array( l(2),l(3),l(4),l(5),l(6),l(7) ) )
	val label = values.last - 1
        LabeledPoint(label,featureVec) 
    }
    
    
    val Array(trainData,testData) = data.randomSplit(Array(0.8,0.2)) */

    val trainRDD = sc.textFile(trainFile)
    val testRDD = sc.textFile(testFile)
    
    val trainData = trainRDD.map{ line =>
	val values = line.split(',').map(_.toDouble)
    	var l = values.init
    	val featureVec = Vectors.dense( Array( l(2),l(3),l(4),l(5),l(6),l(7) ) )
	val label = values.last - 1
        LabeledPoint(label,featureVec) 
    }

    val testData = testRDD.map{ line =>
	val values = line.split(',').map(_.toDouble)
    	var l = values.init
    	val featureVec = Vectors.dense( Array( l(2),l(3),l(4),l(5),l(6),l(7) ) )
	val label = values.last - 1
        LabeledPoint(label,featureVec) 
    }    

    //val Array(trainData,testData) = data.randomSplit(Array(0.8,0.2)) 

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 40000
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 32
    val maxMemoryInMB = 1024

    val strategy = new Strategy(Classification, Gini, maxDepth, numClasses, maxBins, Sort,  Map[Int, Int]())
    strategy.setMaxMemoryInMB(10240)
    //val strategy1 = Strategy.defaultStrategy("Classification")
    val model = DecisionTree.train(trainData,strategy)

    //val model = DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
    	val prediction = model.predict(point.features)
  	(point.label, prediction)
    }

   val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
   println("Test Error = " + testErr)
   println("Learned classification tree model:\n"+model)
  }

  
}
