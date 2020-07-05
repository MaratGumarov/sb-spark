import org.apache.spark.sql.SparkSession

object EsWrite extends App {

  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val esNodes = ""
  val esUser = ""
  val pwd = ""
  val index = ""
  val inputPath = ""
  val modelPath = ""


  val df = spark.read.json(inputPath)

  import org.apache.spark.ml._


  val model = PipelineModel.load(modelPath)

  import org.apache.spark.sql.functions._

  val testing = df
    .select('date, 'uid, explode('visits).alias("visit"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .dropDuplicates(Seq("uid", "domain", "date"))
    .groupBy('uid, 'date).agg(collect_list('host).alias("domains"))
    .select('uid, 'domains, 'date)


  val dfFinal = model.transform(testing)


  val esOptions =
    Map(
      "es.nodes" -> esNodes,
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true",
      "es.net.http.auth.user" -> esUser,
      "es.net.http.auth.pass" -> pwd
    )

  import org.apache.spark.sql.types._

  dfFinal
    .select('uid, ('date / 1000).cast(TimestampType).as("date"), 'category.as("gender_age"))
    // .show
    .write.mode("append")
    .format("es").options(esOptions)
    .save(index + "/_doc")

}
