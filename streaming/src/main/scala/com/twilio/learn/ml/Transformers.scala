package com.twilio.learn.ml

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, MLReader, MLWriter}
import org.slf4j.{Logger, LoggerFactory}



object RatingClassificationTransformer extends DefaultParamsReadable[RatingClassificationTransformer] {
  override def read: MLReader[RatingClassificationTransformer] = super.read
}

class RatingClassificationTransformer(override val uid: String) extends Transformer with DefaultParamsWritable {
  val SafeForKids = Seq(1.0, 6.0,8.0,9.0,11.0)
  val SafeForMost = Seq(0.0,4.0,2.0,12.0)

  def this() = this(Identifiable.randomUID("ratingClassTransformer"))

  override def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = {
    val idx = schema.fieldIndex("rating_index")
    val field = schema.fields(idx)
    if (field.dataType != DoubleType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type DoubleType")
    }
    // Add the return field
    schema.add(StructField("rating_class", DoubleType, false))
  }

  override def copy(extra: org.apache.spark.ml.param.ParamMap): org.apache.spark.ml.Transformer = defaultCopy(extra)

  override def write: MLWriter = super.write
  override def save(path: String): Unit = super.save(path)
  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn("rating_class",
      when(col("rating_index").isInCollection(SafeForKids), 0.0D)
        .when(col("rating_index").isInCollection(SafeForMost), 1.0D)
        .otherwise(2.0D))
  }
}

class IsKidSafeTransformer(override val uid: String) extends Transformer with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("kidSafeTransformer"))

  override def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = {
    val idx = schema.fieldIndex("rating_class")
    val field = schema.fields(idx)
    if (field.dataType != DoubleType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type DoubleType")
    }
    // Add the return field
    schema.add(StructField("label", DoubleType, false))
  }

  override def copy(extra: org.apache.spark.ml.param.ParamMap): org.apache.spark.ml.Transformer = defaultCopy(extra)
  override def write: MLWriter = super.write
  override def save(path: String): Unit = super.save(path)
  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn("label", when(col("rating_class").equalTo(0.0D), 1.0D).otherwise(.0D))
  }

}

object IsKidSafeTransformer extends DefaultParamsReadable[IsKidSafeTransformer] {
  override def read: MLReader[IsKidSafeTransformer] = super.read
}

object Transformers {

  def apply(pipelineLocation: String, modelLocation: String): Transformers = {
    new Transformers(pipelineLocation, modelLocation)
  }

}

class Transformers(
  pipelineLocation: String,
  modelLocation: String
  ) {

  import org.apache.spark.ml.tuning.TrainValidationSplitModel
  import org.apache.spark.ml.PipelineModel

  val logger: Logger = LoggerFactory.getLogger(classOf[Transformers])
  logger.info(s"pipelineLocation=$pipelineLocation and modelLocation=$modelLocation")

  lazy val defrostedPipeline: PipelineModel = PipelineModel.load(pipelineLocation)
  lazy val defrostedModel: TrainValidationSplitModel = TrainValidationSplitModel.load(modelLocation)

  def transform(df: DataFrame): DataFrame = defrostedPipeline.transform(df)
  def predict(df: DataFrame): DataFrame = defrostedModel.transform(df)

}


