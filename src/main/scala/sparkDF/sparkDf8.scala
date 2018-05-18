package sparkDF

import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by alex on 30.3.18.
  */
object sparkDf8 extends App{

  case class Person(name :String, age :Int)

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val peopleRDD = spark.sparkContext.textFile("/home/alex/spark/projects/spark1/src/main/resources/person.txt")
    .map(line =>line.split(","))
    .map(attributes =>Person(attributes(0),attributes(1).trim.toInt) )


  val peopleDF =  peopleRDD.toDF()
  peopleDF.show()

  spark.stop()





}
