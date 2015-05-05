the 'GlobalModelDT.py' runs fine with the data from 'Sample_small_input' folder.

The number of classes give here are 10001.. 

-------------------------------------------------------------------------------------
Howeever there are problems running with actuall dataset the 182 users.
 
Since in this case, we need to give number of possible (values) classes (to be predicted as) as 2500000000 because we considered 50000 to be the possible value for our grid to divide all the users. So the grid is 50000 by 50000 (matrix). thus there will 2500000000 possible values for base stations positioned in the range 0000000000 through 5000050000.

Thus when we try to build a model with numClasses=2500000000 gives the below error.

model = DecisionTree.trainClassifier(trainingData, numClasses=2500000000, categoricalFeaturesInfo={}, impurity='entropy', maxDepth=15, maxBins=100)

************** Traces Start ****************

py4j.protocol.Py4JError: An error occurred while calling o36.trainDecisionTreeModel. Trace:
py4j.Py4JException: Method trainDecisionTreeModel([class org.apache.spark.api.java.JavaRDD, class java.lang.String, class java.lang.Long, class java.util.HashMap, class java.lang.String, class java.lang.Integer, class java.lang.Integer, class java.lang.Integer, class java.lang.Double]) does not exist
	at py4j.reflection.ReflectionEngine.getMethod(ReflectionEngine.java:333)
	at py4j.reflection.ReflectionEngine.getMethod(ReflectionEngine.java:342)
	at py4j.Gateway.invoke(Gateway.java:252)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:133)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:207)
	at java.lang.Thread.run(Thread.java:745)


This means the values we are giving are exceeding the variables limit, so the intrepreter says the method not found.
************** Traces End ****************


------------------------------------------------------------------------------------
Thus, if numClasses (2500000000) value is reduced to numClasses=250000000 or even lesser. there are no such errors as above but there are memory requirements erros as shown below:

File "/home/amar/Desktop/Material/spark/spark-1.2.0-bin-hadoop2.4/python/lib/py4j-0.8.2.1-src.zip/py4j/protocol.py", line 300, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o36.trainDecisionTreeModel.
: java.lang.IllegalArgumentException: requirement failed: RandomForest/DecisionTree given maxMemoryInMB = 256, which is too small for the given features.  Minimum value = 1697
	at scala.Predef$.require(Predef.scala:233)
	at org.apache.spark.mllib.tree.RandomForest.run(RandomForest.scala:191)
	at org.apache.spark.mllib.tree.DecisionTree.run(DecisionTree.scala:64)
	at org.apache.spark.mllib.tree.DecisionTree$.train(DecisionTree.scala:95)
	at org.apache.spark.mllib.api.python.PythonMLLibAPI.trainDecisionTreeModel(PythonMLLibAPI.scala:486)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:231)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:379)
	at py4j.Gateway.invoke(Gateway.java:259)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:133)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.GatewayConnection.run(GatewayConnection.java:207)
	at java.lang.Thread.run(Thread.java:745)

----------------------------------------------------------------------------------

Since integer values in python can take on really big numbers and it is machine dependent. and also from the memory requirement problem point of view, It is better to run to cluster to see if the same error persists.

Also, I could see if can specify, on how much memory to use to be passed as a parameter and try to partition the data(laptop) before running.



