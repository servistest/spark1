package sparkDF

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 30.3.18.
  */
object sparkDF11 extends App {

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  //  !!! Need to set fro join operations !!!
  spark.sqlContext.setConf("spark.sql.crossJoin.enabled", "true")

  import spark.implicits._

  val guestsDF = spark.read.json("/home/alex/spark/projects/spark1/src/main/resources/guests.json")
  guestsDF.printSchema()
  val eventsDF = spark.read.json("/home/alex/spark/projects/spark1/src/main/resources/events.json")
  eventsDF.printSchema()

  eventsDF.join(guestsDF, $"id" === $"user_id")
      .where ($"age" === 25 )
    .select($"name",$"age" ,$"event")
    .show()


}
