import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object filter extends App{
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    val topic = sc.getConf.get("spark.filter.topic_name")
    val offset = sc.getConf.get("spark.filter.offset")
    val target = sc.getConf.get("spark.filter.output_dir_prefix")

    val kafkaParams = Map(
        "kafka.bootstrap.servers" -> "10.0.1.13:6667",
        "subscribe" -> topic
    )

    val inputRaw = spark
      .read.format("kafka")
      .options(kafkaParams)
      .load

    val rawJsons = inputRaw
      .select(col("value").cast("String")).as[String]

    val parsedInput = spark.read.json(rawJsons)
      .withColumn("date", from_unixtime('timestamp/1000, "YYYYMMDD"))

    val buys = parsedInput.filter(col("event_type") === "buy")
    buys.show(5, truncate = false)
    buys.write
      .partitionBy("date")
      .mode("overwrite")
      .parquet(target + "/buy")


    val views = parsedInput.filter(col("event_type") === "view")
    views.show(5, truncate = false)
    views.write
        .partitionBy("month_col", "day_col")
        .mode("overwrite")
        .parquet(target + "/view")
}
