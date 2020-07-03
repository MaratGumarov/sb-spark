import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object test extends App {

  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val modelPath = spark.conf.get("spark.mlproject.model.path")
  val inputTopicName = spark.conf.get("spark.mlproject.input_topic_name")
  val outputTopicName = spark.conf.get("spark.mlproject.output_topic_name")

  val schema = StructType(
    StructField("uid", StringType, nullable = true) ::
      StructField("visits", ArrayType(
        StructType(StructField("timestamp", LongType, nullable = true) ::
          StructField("url", StringType, nullable = true) :: Nil),
        containsNull = true),
        nullable = true) :: Nil)

  val model = PipelineModel.load(modelPath)

  val sdf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.1.13:6667")
    .option("subscribe", inputTopicName)
    .load

  val testing = sdf
    .select(from_json($"value".cast("string"), schema).alias("value"))
    .select(col("value.*"))
    .select('uid, explode('visits).alias("visit"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .dropDuplicates(Seq("uid", "domain"))
    .groupBy('uid).agg(collect_list('host).alias("domains"))
    .select('uid, 'domains)

  val dfFinal = model.transform(testing)

  val sink = createKafkaSink(dfFinal.select('uid, 'category.alias("gender_age")))
  val sq = sink.start()
  sq.awaitTermination()

  def createKafkaSink(df: DataFrame) = {
    df.toJSON.writeStream
      .outputMode("update")
      .format("kafka")
      .option("checkpointLocation", "chk/svetlana_akselrod")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("topic", outputTopicName)
      .trigger(Trigger.ProcessingTime("5 seconds"))
  }
}
