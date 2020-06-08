import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ClientsShop(spark: SparkSession, properties: Properties) {
  def create(): DataFrame = {
    val esOptions =
      Map(
        "es.nodes" -> "10.0.1.9:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true"
      )

    val categories = spark.read.format("es")
      .options(esOptions)
      .load("visits")

    val esWideTmp = categories
      .select(col("uid"), col("category"))
      .na.drop
      .withColumn("d", lit(1))
      .groupBy(col("uid"))
      .pivot(col("category"))
      .agg(sum(col("d")))

    esWideTmp
      .toDF(esWideTmp.columns.map("shop_" + _).map(_.toLowerCase).map(_ replaceAll("-", "_")).toSeq: _*)
      .withColumnRenamed("shop_uid", "uid")
  }
}
