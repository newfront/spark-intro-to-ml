package com.twilio.learn.ml

import org.apache.spark.sql.DataFrame

object LinearRegressionTrainer {

  import org.apache.spark.ml.evaluation.RegressionEvaluator
  import org.apache.spark.ml.regression.LinearRegression
  import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

  def train(df: DataFrame, writePath: String): Unit = {
    val lr = new LinearRegression()
      .setMaxIter(10)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)
      // Evaluate up to 2 parameter settings in parallel
      .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(df)
    model.write.overwrite.save(writePath)
  }

}
