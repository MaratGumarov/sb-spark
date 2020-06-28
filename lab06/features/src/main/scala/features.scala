import org.apache.spark.sql.SparkSession

object features extends App {

  val spark = SparkSession.builder().getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val userItems = spark.read.parquet("users-items/20200429")
  val weblogs = spark.read.json("/labs/laba03")

  import org.apache.spark.sql.functions._

  val exploded_logs = weblogs
    .withColumn("expl", explode(col("visits")))
    .select(col("uid"), col("expl.url"), col("expl.timestamp"))

  val parsed = exploded_logs
    .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
    .withColumn("domain", regexp_replace(col("host"), "www.", ""))

  val top1000domains = parsed
    .select(col("domain"))
    .groupBy(col("domain"))
    .agg(count("domain").as("count"))
    .orderBy(desc("count"))
    .limit(1000)

  val reach =
    userItems.join(parsed, Seq("uid"), "left")
      .select(
        col("uid"),
        col("domain"),
        col("timestamp"),
        concat(lit("web_day_"), lower(date_format(to_timestamp(col("timestamp") / 1000), "EEE"))).as("DAY"),
        concat(lit("web_hour_"), lower(date_format(to_timestamp(col("timestamp") / 1000), "H"))).as("HOUR"),
        date_format(to_timestamp(col("timestamp") / 1000), "H").as("H").cast("Int")
      )
      .withColumn("work",
        when(col("H") >= 9 && col("H") < 18, "work_hours")
          .otherwise(when(col("H") >= 18, "evening_hours")
            .otherwise("other")))

  val days = reach
    .groupBy("uid")
    .pivot("DAY")
    .agg(count("uid"))
    .na.fill(0)
    .drop("null")


  val hours = reach
    .groupBy("uid")
    .pivot("HOUR")
    .agg(count("uid"))
    .na.fill(0)
    .drop("null")

  val workHoursq = reach
    .na.drop(Seq("work"))
    .groupBy("uid")
    .pivot("work")
    .agg(count("uid"))
    .na.fill(0)

  val workHours = workHoursq
    .withColumn("web_fraction_evening_hours", col("evening_hours") / (col("other") + col("evening_hours") + col("work_hours")))
    .withColumn("web_fraction_work_hours", col("work_hours") / (col("other") + col("evening_hours") + col("work_hours")))
    .drop(col("other"))
    .drop(col("evening_hours"))
    .drop(col("work_hours"))

  import org.apache.spark.ml.feature._
  import org.apache.spark.sql.functions._

  println("parsed.count " + parsed.count)
  println("top1000 " + top1000domains.count)

  val joined = parsed.join(top1000domains, Seq("domain"))
  println("joined " + joined.count)

  val feat = joined

    .groupBy("uid")
    .pivot("domain")
    .agg(count(col("*")))
    .na.fill(0)

  val aggregated = feat
    .select(col("uid"), array(feat.columns.filter(c => c!="uid").map(c=>col("`" + c + "`")):_*).as("domain_features"))


  val all =
    userItems
      .join(aggregated, Seq("uid"), "left")
      .join(days, Seq("uid"), "left")
      .join(hours, Seq("uid"), "left")
      .join(workHours,Seq("uid"), "left")

  all
    .write
    .mode("overwrite")
    .parquet("features")
}
