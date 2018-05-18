package book

import book.example2.sparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
/**
  * Created by alex on 3.5.18.
  */
object example8 extends App {


  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()

  val logRDD=  sparkSession.sparkContext.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log3.txt"))

  val accumulator =  sparkSession.sparkContext.longAccumulator("accum")
  val words = logRDD.flatMap { line =>
    {
      if (line == "") {
         accumulator.add(1)
      }
    line.split(" ")

    }
  }


  // so map is lazy transformation - we need first words.foreach and then  println(accumulator.value)
  words.foreach(println)

  println(accumulator.value)

   // count of empty strings
//  logRDD.

}
