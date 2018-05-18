package sparkDF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by alex on 29.3.18.
  */
object sparkDF_TEst extends App {

  case class Person(name: String, age: String)

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()


  import spark.implicits._

  // Create an RDD of Person objects from a text file, convert it to a Dataframe
  val peopleDF = spark.sparkContext
    .textFile("/home/alex/spark/projects/spark1/src/main/resources/person.txt")
    .map(_.split(","))
    .map(attributes => Person(attributes(0), attributes(1)))
    .toDF()


//   Register the DataFrame as a temporary view
  peopleDF.createOrReplaceTempView("people")
//
//   SQL statements can be run by using the sql methods provided by Spark
  val ageTemp = 10
  val teenagersDF = spark.sql(s"SELECT name, age FROM people where age > $ageTemp and age < 30")
//
  teenagersDF.show()


//   The columns of a row in the result can be accessed by field index

  teenagersDF.map(teenager => "Name " + teenager(0)).show()
  teenagersDF.map(teenager => "Age " + teenager(1)).show()

  // or by field name

  teenagersDF.map(teenager => "Name - " + teenager.getAs[String]("name")).show()
  teenagersDF.map(teenager => "Age - " + teenager.getAs[Int]("age")).show()


////
//  teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
////
//  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//
//  teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
}
