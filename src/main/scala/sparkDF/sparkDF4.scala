package sparkDF

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 29.3.18.
  */
object sparkDF4 extends App{
  case class Person(name: String, age: Long)

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val peopleDF = spark.sparkContext
    .textFile("/home/alex/spark/projects/spark1/src/main/resources/person.txt")
      .map(line => line.split(","))
    .map(attributes => Person(attributes(0),attributes(1).trim.toInt))
    .toDF()


    peopleDF.createOrReplaceTempView("people")

  val teenagerDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

  teenagerDF.map(teenager => "Name: " + teenager(0)).show()


  spark.stop()

}
