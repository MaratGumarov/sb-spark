import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Domains(spark: SparkSession, properties: Properties) {
  def create(): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", properties.getProperty("postgres.host"))
      .option("driver", "org.postgresql.Driver")
      .option("user", properties.getProperty("postgres.user"))
      .option("password", properties.getProperty("postgres.pwd"))
      .option("dbtable", "domain_cats")
      .load()
  }
}