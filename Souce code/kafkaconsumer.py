from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
import os

spark.sparkContext.setLogLevel("ERROR")
df = spark \
  .readStream \
  .format("kafka") \
  .option("failOnDataLoss", "false") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "weather") \
  .option("startingOffsets", "latest") \
  .load()


df.printSchema()
df=df.selectExpr("CAST(value as STRING)")

datasett = "age INT,sex INT,cp INT,trestbps INT,chol INT,fbs INT,restecg INT,thalach INT,exang INT,oldpeak FLOAT,slope INT,ca INT,thal INT,target INT"
df2 = df \
        .select(from_csv(col("value"), datasett) \
                .alias("heart"))
df3 = df2.select("heart.*")
df3.printSchema()
print("working")


from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
training = spark.read.csv("/opt/spark/spark-3.1.2-bin-hadoop3.2/hd/heart.csv", inferSchema=True, header=True)
assembler = VectorAssembler(inputCols=['age',
 'sex',
 'cp',
 'trestbps',
 'chol',
 'fbs',
 'restecg',
 'thalach',
 'exang',
 'oldpeak',
 'slope',
 'ca',
 'thal'
 ],outputCol="features")
 
 
output = assembler.transform(training)
df_final = output.select("features","target")
print(df_final)
train, test = df_final.randomSplit([0.7,0.3])
lr = LogisticRegression(labelCol="target").fit(train)
tr = lr.evaluate(train).predictions
results = lr.evaluate(test).predictions
tp = results[(results.target == 1) & (results.prediction == 1)].count()
tn = results[(results.target == 0) & (results.prediction == 0)].count()
fp = results[(results.target == 0) & (results.prediction == 1)].count()
fn = results[(results.target == 1) & (results.prediction == 0)].count()
accuracylogistic = float((tp+tn)/(results.count()))
print(accuracylogistic)

#random forest

from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'target',)
rfModel = rf.fit(train)
predictionrf = rfModel.transform(test)
tpr = predictionrf[(predictionrf.target == 1) & (predictionrf.prediction == 1)].count()
tnr = predictionrf[(predictionrf.target == 0) & (predictionrf.prediction == 0)].count()
fpr = predictionrf[(predictionrf.target == 0) & (predictionrf.prediction == 1)].count()
fnr = predictionrf[(predictionrf.target == 1) & (predictionrf.prediction == 0)].count()
accuracyr = float((tpr+tnr)/(predictionrf.count()))
print(accuracyr)

#linear regression

from pyspark.ml.regression import LinearRegression
lir = LinearRegression(featuresCol = 'features', labelCol='target', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lir_model = lir.fit(train)
predictionlir = lir_model.transform(test)
tplir = predictionlir[(predictionlir.target == 1) & (predictionlir.prediction == 1)].count()
tnlir = predictionlir[(predictionlir.target == 0) & (predictionlir.prediction == 0)].count()
fplir = predictionlir[(predictionlir.target == 0) & (predictionlir.prediction == 1)].count()
fnlir = predictionlir[(predictionlir.target == 1) & (predictionlir.prediction == 0)].count()
accuracylir = float((tplir+tnlir)/(predictionlir.count()))
print(accuracylir)


#onevsrest

from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
lr = LogisticRegression(featuresCol = 'features', labelCol='target', maxIter=10, tol=1E-6, fitIntercept=True)
ovr = OneVsRest(featuresCol = 'features', labelCol='target',classifier=lr)
ovrModel = ovr.fit(train)
predictions = ovrModel.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName="accuracy")
accuracyonevsrest = evaluator.evaluate(predictions)
print(accuracyonevsrest)
print("Test Error = %g" % (1.0 - accuracyonevsrest))

#linear svm

from pyspark.ml.classification import LinearSVC
lsvc = LinearSVC(featuresCol = 'features', labelCol='target', maxIter=10, regParam=0.1)
lsvcModel = lsvc.fit(train)
predictionlsv = lsvcModel.transform(test)
tplsv = predictionlsv[(predictionlsv.target == 1) & (predictionlsv.prediction == 1)].count()
tnlsv = predictionlsv[(predictionlsv.target == 0) & (predictionlsv.prediction == 0)].count()
fplsv = predictionlsv[(predictionlsv.target == 0) & (predictionlsv.prediction == 1)].count()
fnlsv = predictionlsv[(predictionlsv.target == 1) & (predictionlsv.prediction == 0)].count()
accuracylsv = float((tplsv+tnlsv)/(predictionlsv.count()))
print(accuracylsv)

#gbt

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
data = sc.textFile("/opt/spark/spark-3.1.2-bin-hadoop3.2/hd/heart1.txt")
def parsePoint(line):
    values = [float(s) for s in line.strip().split(',')]
    if values[0] == -1: 
        values[0] = 0
    elif values[0] > 0:
        values[0] = 1
    return LabeledPoint(values[0], values[1:])

parsed_data = data.map(parsePoint)
(trainingData, testData) = parsed_data.randomSplit([0.7, 0.3])
model = GradientBoostedTrees.trainClassifier(trainingData,
                                             categoricalFeaturesInfo={}, numIterations=10)

predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(
    lambda lp: lp[0] != lp[1]).count() / float(testData.count())
print(1-testErr)    
print('Test Error = ' + str(testErr))

print('Learned classification GBT model:')
print(model.toDebugString())

#decision tree
from pyspark.mllib.tree import DecisionTree
modeldt = DecisionTree.trainClassifier(trainingData, numClasses=5, categoricalFeaturesInfo={}, impurity='gini', maxDepth=3, maxBins=32)

predictionsdt = modeldt.predict(testData.map(lambda x: x.features))
labelsAndPredictionsdt = testData.map(lambda lp: lp.label).zip(predictionsdt)
testErrdt = labelsAndPredictionsdt.filter(lambda x: x[0]!=x[1]).count() / float(testData.count())
print('Test Error = ' + str(testErrdt))

print(modeldt.toDebugString())


modeldtr = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo={}, impurity='variance', maxDepth=3, maxBins=32)


predictionsdtr = modeldtr.predict(testData.map(lambda x: x.features))
labelsAndPredictionsdtr = testData.map(lambda lp: lp.label).zip(predictionsdtr)
testMSEdtr = labelsAndPredictionsdtr.map(lambda x : (x[0] - x[1]) * (x[0] - x[1])).sum() / float(testData.count())
print('Test Mean Squared Error = ' + str(testMSEdtr))

print(modeldtr.toDebugString())





a = assembler.transform(df3)
b=a.select("features","target")
predictionrf = rfModel.transform(b)
query1 = predictionrf.select("prediction")
query = query1 \
  .selectExpr("to_json(struct(*)) AS value") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "testing") \
  .option("checkpointLocation", "/opt/spark/spark-3.1.2-bin-hadoop3.2/hd/") \
  .start()
  

query.awaitTermination()


#result.awaitTermination()
#result2.awaitTermination()
#result9.awaitTermination()
