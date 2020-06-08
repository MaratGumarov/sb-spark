import org.apache.spark.sql.SparkSession

object data_mart extends App {

  val properties = ConfigurationReader.conf

  val postgresUrl = properties.getProperty("postgres.url")
  val postrgresUser = properties.getProperty("postgres.user")
  val postrgresPwd = properties.getProperty("postgres.pwd")

  val elasticNodes = properties.getProperty("elastic.nodes")

  val spark = SparkSession
    .builder()
    .getOrCreate()

  val clients = Clients(spark, properties).create()
  val clientsShop = ClientsShop(spark, properties).create()
  val domains = Domains(spark, properties).create()
  val clientsWeb = ClientsWeb(spark, domains, properties).create()

  val dataMart = clients
    .join(clientsShop, Seq("uid"), "left")
    .join(clientsWeb, Seq("uid"), "left")

  Target(spark, dataMart, properties).store()
}
