package book

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 3.5.18.
  */
object example9_master extends App {


  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()


  val input = sparkSession.sparkContext.textFile("/home/alex/spark/projects/spark1/src/main/resources/guests.json")




}
