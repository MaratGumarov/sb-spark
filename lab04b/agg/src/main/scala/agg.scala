import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object agg extends App {

  override def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs.delete(new Path("chk"), true)

    val schema = StructType(
      StructField("event_type", StringType) ::
        StructField("category", StringType) ::
        StructField("item_id", StringType) ::
        StructField("item_price", StringType) ::
        StructField("uid", StringType) ::
        StructField("timestamp", LongType) ::
        Nil
    )

//    val options = Map(
//      "kafka.bootstrap.servers" -> "10.0.1.13:6667",
//      "subscribe" -> "lab04b_input_data",
//      "startingOffsets" -> "earliest",
//      //         "startingOffsets" -> "latest",
//      "maxOffsetsPerTrigger" -> "100"
//    )


    val options = Map(
      "kafka.bootstrap.servers" -> "10.0.1.13:6667",
      "subscribe" -> "marat_gumarov",
//               "startingOffsets" -> "earliest",
      "startingOffsets" -> "latest",
      "maxOffsetsPerTrigger" -> "100"
    )

    val fromKafka = spark
      .readStream
      .format("kafka")
      .options(options = options)
      .load

    val parsed = fromKafka.withColumn("value", from_json(col("value").cast(StringType), schema))


    val groupDF = parsed
             .withColumn("value.timestamp", col("value.timestamp") / lit(1000) cast LongType)
      .withColumn("ts", to_timestamp(col("value.timestamp") / lit(1000)))
      .withColumn("value.item_price", col("value.item_price").cast(LongType))
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("ts"), "1 hour"))
      .agg(
        sum(when(expr("value.event_type = 'buy'"),
          col("value.item_price")).otherwise(lit(0)))
          as "revenue",
        count("value.uid") as "visitors",
        sum(when(expr("value.event_type = 'buy'"),
          lit(1)).otherwise(lit(0)))
          as "purchases")
      .withColumn("aov", col("revenue") / col("purchases"))
      .withColumn("start_ts",col("window.start") cast LongType)
      .withColumn("end_ts",col("window.end") cast LongType)
      .select("start_ts", "end_ts", "revenue", "purchases", "visitors", "aov")


    val dfWriter = groupDF
      .toJSON
      .withColumn("topic", lit("marat_gumarov_lab04b_out"))
      .writeStream
      .outputMode("update")
      .format("kafka")
             .trigger(Trigger.ProcessingTime("10 seconds"))
//      .trigger(Trigger.Once)
      .outputMode("complete")
      .option("kafka.bootstrap.servers", "10.0.1.13:6667")
      .option("checkpointLocation", s"chk/lab04b")
      .start

    while (true) {
      dfWriter.awaitTermination(10000)
    }

  }
}