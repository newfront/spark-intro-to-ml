package com.twilio.learn

import com.twilio.learn.config.AppConfig
import com.twilio.learn.ml.Transformers
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

@SerialVersionUID(10L)
case class NetworkCommand(show_id: String) extends Serializable

object PredictionStream extends App {
  val logger: Logger = LoggerFactory.getLogger(classOf[PredictionStream])

  assert(args.length > 0, "StreamingPredictionsApp expects path to app.yaml in args")
  // added for unit test access to path
  protected[learn] var _configLocation = if (args.length > 0) args(0) else ""
  @transient val appConfig: AppConfig = AppConfig.parse(_configLocation)

  // https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-SparkSession.html
  def createRuntimeSparkSession(): SparkSession = {
    // note: the spark config will automatically load from the -Dspark properties or sys.env
    logger.info(s"createSparkSession:runtime")


    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("spark-command-controller")
      .setJars(SparkContext.jarOfClass(classOf[PredictionStream]).toList)

    val session = SparkSession.builder
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    session
  }

  def apply(config: AppConfig,  spark: SparkSession): PredictionStream = {
    new PredictionStream(config, spark)
  }

  @transient lazy val session: SparkSession = createRuntimeSparkSession()
  PredictionStream(appConfig, session).run()

}

@SerialVersionUID(10L)
class PredictionStream(config: AppConfig, val spark: SparkSession) extends Serializable {
  import scala.util.{Failure, Success, Try}
  val logger: Logger = LoggerFactory.getLogger(classOf[PredictionStream])

  val checkpointLocation: String = spark.conf.get("spark.command.controller.checkpointLocation", "")

  val redisStreamFormat: StructType = {
    val streamDataDDL = "`show_id` BIGINT"
    StructType.fromDDL(streamDataDDL)
  }

  lazy val inputStream: DataStreamReader = {
    spark.readStream
      .format("redis")
      .option("stream.keys", "v1:movies:test:kidSafe")
      .schema(redisStreamFormat)
  }

  def joinData(): DataFrame = {
    val netflixCategoryDataWithRatingSchema = "`show_id` BIGINT,`category` STRING, `rating` STRING"
    val netflixCategoryStructSchema = StructType.fromDDL(netflixCategoryDataWithRatingSchema)
    spark.read
      .format("org.apache.spark.sql.redis")
      .schema(netflixCategoryStructSchema)
      .option("key.column", "show_id")
      .option("table", "netflix_category_rating")
      .load()
  }

  def run(): Unit = {
    lazy val contentWithCategories = joinData()
    val processingTimeTrigger = Trigger.ProcessingTime("2 seconds")

    /*
    spark.transformers.pipelineLocation: "conf/pipeline"
    spark.transformers.modelLocation: "conf/lrm"
     */

    val pipelineLocation = spark.conf.get("spark.transformers.pipelineLocation", "conf/pipeline")
    val modelLocation = spark.conf.get("spark.transformers.modelLocation", "conf/lrm")
    val transformers = Transformers(pipelineLocation, modelLocation)

    val query: StreamingQuery = inputStream
      .load()
      .join(contentWithCategories, Seq("show_id"))
      .transform(transformers.transform)
      .transform(transformers.predict)
      .writeStream
      .foreachBatch( (data: Dataset[_], batch: Long) => {
        data
            .select("show_id", "category", "rating", "prediction")
          .write
          .format("org.apache.spark.sql.redis")
          .option("table", "v1:movies:test:kidSafe:predict")
          .option("key.column", "show_id")
          .mode("ignore") // [errorIfExists, overwrite, append, ignore] (overwrite will overwrite the full table)
          .save()
      })
      .queryName("predictions")
      .outputMode(OutputMode.Append())
      //.option("checkpointLocation", checkpointLocation)
      .trigger(processingTimeTrigger)
      .start()

    query.awaitTermination()
  }
}
