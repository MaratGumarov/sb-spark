import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Target(spark: SparkSession, dataMart: DataFrame, properties: Properties) {
  def store(): Unit = {
    dataMart
      .write
      .format("jdbc")
      .option("dbtable", "clients")
      .option("url", properties.getProperty("postgres.target.url"))
      .option("driver", "org.postgresql.Driver")
      .option("user", properties.getProperty("postgres.user"))
      .option("password", properties.getProperty("postgres.pwd"))
      .mode("overwrite")
      .save
  }
}
