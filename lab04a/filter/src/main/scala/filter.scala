import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object filter extends App {
  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val topic = sc.getConf.get("spark.filter.topic_name")
  val offset = sc.getConf.get("spark.filter.offset")
  val target = sc.getConf.get("spark.filter.output_dir_prefix")
  val fs = FileSystem.get(URI.create(target), spark.sparkContext.hadoopConfiguration)
  fs.delete(new Path(target), true)

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "10.0.1.13:6667",
    "subscribe" -> topic,
    "startingOffsets" -> {
      if (offset == "earliest")
        offset
      else
        s""" { "$topic": { "0": $offset } } """
    }
  )

  val inputRaw = spark
    .read.format("kafka")
    .options(kafkaParams)
    .load

  val rawJsons = inputRaw
    .select(col("value").cast("String")).as[String]

  val parsedInput = spark.read.json(rawJsons)
    .withColumn("date", from_unixtime('timestamp / 1000, "YYYYMMdd"))
    .withColumn("date_part", from_unixtime('timestamp / 1000, "YYYYMMdd"))

  val buys = parsedInput.filter(col("event_type") === "buy")
  buys.write
    .partitionBy("date_part")
    .mode("overwrite")
    .json(target + "/buy")


  val views = parsedInput.filter(col("event_type") === "view")
  views.show(5, truncate = false)
  views.write
    .partitionBy("date_part")
    .mode("overwrite")
    .json(target + "/view")

  val path = new Path(target)

  fs
    .listStatus(path)
    .filter(_.isDirectory)
    .map(_.getPath)
    .flatMap(d => fs.listStatus(d))
    .filter(_.isDirectory)
    .map(_.getPath)
    .map(s => fs.rename(s, new Path(s.getParent, s.getName.replace("date_part=", ""))))

}
