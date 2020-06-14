import org.apache.spark.SparkContext

object filter extends App{
    val spark = SparkContext.getOrCreate()


    //spark-submit
    // --conf spark.filter.topic_name=lab04_input_data
    // --conf spark.filter.offset=earliest
    // --conf spark.filter.output_dir_prefix=/user/name.surname/visits
    // --class filter
    // --packages
    //      org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
    //      .target/scala-2.11/filter_2.11-1.0.jar
    println("spark.filter.topic_name=" + spark.getConf.get("spark.filter.topic_name"))
    println("filter.topic_name" + spark.getConf.get("filter.topic_name"))

//    val kafkaParams = Map(
//        "kafka.bootstrap.servers" -> "10.0.1.13:6667",
//        "subscribe" -> "lab04_input_data"
}
