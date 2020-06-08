import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ClientsWeb(spark: SparkSession, domains: DataFrame, properties: Properties) {
  def create(): DataFrame = {
    spark
      .read
      .json(properties.getProperty("weblogs.path"))
      .withColumn("expl", explode(col("visits")))
      .select(col("uid"), col("expl.url"))
      .selectExpr("uid", "parse_url(url, 'HOST') as domain")
      .join(domains, Seq("domain"), "full")
      .select(col("uid"), col("category"))
      .withColumn("d", lit(1))
      .groupBy(col("uid"))
      .pivot(col("category"))
      .agg(sum(col("d")))
      .drop("null")
  }
}
