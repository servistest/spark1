package book

import book.example4.logRDD
import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 28.4.18.
  */
object example5 extends App {

  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()
  val logRDD = sparkSession.sparkContext.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log3.txt"))

  val pairsRDD = logRDD
    .map(line => (line.split(" ")(0), line))

    .cache()


  //  val result = logRDD.map(line => (line.split(" ")(0),line))

  println(pairsRDD.count())
  pairsRDD.foreach(println)

  println(" pairsRDD.filter(  {case (key,value) =>  value.length<4} ")
  pairsRDD.filter(
    {
      case (key, value) => value.length > 63
      case (key, value ) => value.contains("ERROR")
    }

  )
    .foreach(println)


}
