import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Clients(spark: SparkSession, properties: Properties) {

  spark.conf.set("spark.cassandra.connection.host", properties.getProperty("cassandra.host"))
  spark.conf.set("spark.cassandra.connection.port", properties.getProperty("cassandra.port"))
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  def create(): DataFrame = {
    val rawData = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

    val age_cat = udf((age: Int) =>

      age match {
        case x if x >= 18 && x <= 24 => "18-24"
        case x if x >= 25 && x <= 34 => "25-34"
        case x if x >= 35 && x <= 44 => "35-44"
        case x if x >= 45 && x <= 54 => "45-54"
        case x if x >= 55 => ">=55"
        case _ => "other"
      })

    rawData
      .withColumn("age_cat", age_cat(col("age")))
      .select(col("uid"), col("gender"), col("age_cat"))
  }
}
