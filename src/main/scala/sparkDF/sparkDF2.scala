package sparkDF

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 29.3.18.
  */

object sparkDF2 extends App {

  case class Person(name: String, age: Int) {}

  val spark = SparkSession
    .builder()
      .appName("Read persons from json-file")
    .master("local[2]")
    .getOrCreate()

  val df = spark.read.json("src/main/resources/persons.json")
  df.show()

  import spark.implicits._

  df.groupBy("age").count().show()

  // Register the DataFrame as a SQL temporary view
  df.createOrReplaceTempView("people")

  val sqlDF = spark.sql("Select * from people")
  sqlDF.show()

  spark.sql("Select name,salary from people").show()

  // Global temporary view

  df.createOrReplaceGlobalTempView("people")

  spark.sql("Select name, salary from global_temp.people").show()






}
