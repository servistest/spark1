package sparkDF

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by alex on 30.3.18.
  */
object sparkDF7 extends App{

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._


  val peopleRDD = spark.sparkContext.textFile("/home/alex/spark/projects/spark1/src/main/resources/person.txt")

  // The schema is encoded in a string
  val schemaString = "name age"

  // Generate the schema based on the string of schema
  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)

  // Convert records of the RDD (people) to Rows
  val rowRDD = peopleRDD
    .map(_.split(","))
    .map(attributes => Row(attributes(0), attributes(1).trim))

  // Apply the schema to the RDD
  val peopleDF = spark.createDataFrame(rowRDD, schema)

}
