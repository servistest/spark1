package sparkDF

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 29.3.18.
  */
object sparkDF1 extends App{

  case class Person (name:String,age:Int){
  }

  val spark = SparkSession
    .builder()
    .appName("Spark SQL Example")
      .master("local[1]")
//    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  // For implicit conversions like converting RDDs to DataFrames
//  and This import is needed to use the $-notation
  import spark.implicits._

  val df = spark.read.json("src/main/resources/people.json")
  df.show()

  df.printSchema()

  df.select("name").show()
  df.select($"name" , $"age" +1, ($"salary")).show()
  df.filter($"age" >30).show()
  println(df.groupBy("age"))
  df.groupBy("age").count().show()
  df.groupBy("age").sum().show()


}
