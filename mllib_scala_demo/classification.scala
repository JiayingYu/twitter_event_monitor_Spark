/* Spark MLlib demo. The script trains couple classification models and 
 * performs tests on the kaggle dataset */

val rawData = sc.textFile("../spark_test/kaggle/train_noheader.tsv")
val records = rawData.map(line => line.split("\t"))
records.first()

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

/* extract the label variable from the last column and an array of features for colomn 5 to 25 after 
 * cleaning and dealing with missing values */
val data = records.map{ r =>
	val trimmed = r.map(_.replaceAll("\"", ""))
	val label = trimmed(r.size - 1).toInt
	val features = trimmed.slice(4, r.size  -1).map(d => if (d == "?") 0.0 else d.toDouble)
	LabeledPoint(label, Vectors.dense(features))
}

data.cache
val numData = data.count

/* input features vectors for the naive Bayes model by setting negative
 * feature values to zero */
val nbData = records.map{ r =>
	val trimmed = r.map(_.replaceAll("\"", ""))
	val label = trimmed(r.size - 1).toInt
	val features = trimmed.slice(4, r.size  -1).map(d => if (d == "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)
	LabeledPoint(label, Vectors.dense(features))
}

/* trainig classification models */
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.SVMWithSGD

val numIterations = 10

val nbModel = NaiveBayes.train(nbData)
val lrModel = LogisticRegressionWithSGD.train(data, numIterations)
val svmModel = SVMWithSGD.train(data, numIterations)

//predictions
val dataPoint = data.first
val prediction = lrModel.predict(dataPoint.features)

val trueLabel = dataPoint.label

val predictions = lrModel.predict(data.map(lp => lp.features))
predictions.take(5)

//evaluation
val lrTotalCorrect = data.map{point =>
	if (lrModel.predict(point.features) == point.label) 1 else 0
}.sum
val lrAccuracy = lrTotalCorrect / data.count

//compute area under the PR and ROC courves for logistic regression model
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
val metrics = Seq(lrModel, svmModel).map { model =>
	val scoreAndLabels = data.map { point =>
		(model.predict(point.features), point.label)
	}
	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
	(model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
}

// feature standardization
import org.apache.spark.mllib.linalg.distributed.RowMatrix
val vectors = data.map(lp => lp.features)
val matrix = new RowMatrix(vectors)
val matrixSummary = matrix.computeColumnSummaryStatistics()

println(matrixSummary.mean)
println(matrixSummary.min)
println(matrixSummary.max)
println(matrixSummary.variance)
println(matrixSummary.numNonzeros)

import org.apache.spark.mllib.feature.StandardScaler
val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
println(scaledData.first.features)

//retrain model with the standardized data
val lrModelScaled = LogisticRegressionWithSGD.train(scaledData, numIterations)
val lrTotalCorrectScaled = scaledData.map {point =>
	if (lrModelScaled.predict(point.features) == point.label) 1 else 0
}.sum
val lrAccurcyScaled = lrTotalCorrectScaled / numData
val lrPredictionsVsTrue = scaledData.map { point =>
	(lrModelScaled.predict(point.features), point.label)
}
val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
val lrPr = lrMetricsScaled.areaUnderPR //0.7272
val lrRoc = lrMetricsScaled.areaUnderROC // 0.6197

//add additional features: form a mapping of index to category, 1-of-k encoding for column 4 in the dataset
val categories = records.map(r => r(3)).distinct.collect.zipWithIndex.toMap
val numCategories = categories.size

//create a LabelPoint object
val dataCategories = records.map{r =>
	val trimmed = r.map(_.replaceAll("\"", ""))
	val label = trimmed(r.size - 1).toInt
	val categoryIdx = categories(r(3))
    val categoryFeatures = Array.ofDim[Double](numCategories)
    categoryFeatures(categoryIdx) = 1.0
    val otherFeatures = trimmed.slice(4, r.size - 1).map(d => if
    (d == "?") 0.0 else d.toDouble)
    val features = categoryFeatures ++ otherFeatures
    LabeledPoint(label, Vectors.dense(features))
}

println(dataCategories.first)

//standardize new features
val scalerCats = new StandardScaler(withMean = true, withStd = true).fit(dataCategories.map(lp => lp.features))
val scaledDataCats = dataCategories.map(lp =>
LabeledPoint(lp.label, scalerCats.transform(lp.features)))
println(dataCategories.first.features)
println(scaledDataCats.first.features)

//retrain model with expanded feature set
val lrModelScaledCats = LogisticRegressionWithSGD.train(scaledDataCats, numIterations)
val lrTotalCorrectScaledCats = scaledDataCats.map { point =>
    if (lrModelScaledCats.predict(point.features) == point.label) 1 else 0
}.sum
val lrAccuracyScaledCats = lrTotalCorrectScaledCats / numData
val lrPredictionsVsTrueCats = scaledDataCats.map { point =>
    (lrModelScaledCats.predict(point.features), point.label)
}
val lrMetricsScaledCats = new BinaryClassificationMetrics(lrPredictionsVsTrueCats)
val lrPrCats = lrMetricsScaledCats.areaUnderPR
val lrRocCats = lrMetricsScaledCats.areaUnderROC
println(f"${lrModelScaledCats.getClass.getSimpleName}\nAccuracy:${lrAccuracyScaledCats * 100}%2.4f%%\nArea under PR: ${lrPrCats * 100.0}%2.4f%%\nArea under ROC: ${lrRocCats * 100.0}%2.4f%%")


