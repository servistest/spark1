package sparkDF

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import sparkDF.sparkDF4.spark

/**
  * Created by alex on 29.3.18.
  */
object sparkDF5 extends App {

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val peopleRDD = spark.sparkContext.textFile("/home/alex/spark/projects/spark1/src/main/resources/person.txt")
  val rowRDD = peopleRDD.
    map(line => line.split(","))
  .map(attributes => Row(attributes(0),attributes(1).trim))

  val schema =StructType(
    Array(StructField("name",StringType),
      StructField("age",IntegerType)
    )
  )

  val peopleDF= spark.createDataFrame(rowRDD,schema)


  peopleDF.show()




}
