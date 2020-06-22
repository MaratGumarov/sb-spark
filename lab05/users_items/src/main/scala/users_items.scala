import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object users_items extends App {

    val spark = SparkSession.builder().getOrCreate()
    val context = spark.sparkContext
    val conf = context.getConf

    val inputDir = conf.get("spark.users_items.input_dir")
    val outputDir = conf.get("spark.users_items.output_dir")
    val mode = conf.get("spark.users_items.update")

    val inputRawBuy = spark.read.json(inputDir + "/buy/[0-9]*")
    val inputRawView = spark.read.json(inputDir + "/view/[0-9]*")

    val fs = FileSystem.get(URI.create(inputDir), spark.sparkContext.hadoopConfiguration)
    fs.listFiles(new Path(inputDir), false)

    val maxDate = fs.listStatus(new Path(inputDir + "/view"))
      .toList
      .filter(_.isDirectory)
      .map(_.getPath.getName)
      .max

    import org.apache.spark.sql.functions._

    val aggregated_buy = inputRawBuy
      .withColumn("item_id_norm",
          regexp_replace(
              regexp_replace(lower(col("item_id")),
                  "-| ",
                  "_"),
              "^",
              "buy_"))
      .groupBy("uid")
      .pivot("item_id_norm")
      .agg(count(col("item_id_norm")))

    val aggregated_view = inputRawView
      .withColumn("item_id_norm",
          regexp_replace(
              regexp_replace(lower(col("item_id")),
                  "-| ",
                  "_"),
              "^",
              "view_"))
      .groupBy("uid")
      .pivot("item_id_norm")
      .agg(count(col("item_id_norm")))

    val aggregated = aggregated_buy.join(aggregated_view,Seq("uid"),"outer")

    aggregated
        .na.fill(0, aggregated.columns)
      .write
      .mode("overwrite")
      .parquet(outputDir + "/" + maxDate)
}
