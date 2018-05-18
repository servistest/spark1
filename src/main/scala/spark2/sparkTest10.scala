package spark2

import org.apache.spark.sql.SparkSession
import spark2.sparkTest9.{ds, file, sparkContext, sparkSession}

/**
  * Created by alex on 27.3.18.
  */
object sparkTest10 extends App{
  val  file = "/home/alex/spark/projects/spark1/src/main/scala/spark2/data3.txt"

  val sparkSession = SparkSession.builder().master("local[4]").appName("spark session Data Set ").getOrCreate()
  val sparkContext = sparkSession.sparkContext


  import sparkSession.implicits._
  val rdd = sparkContext.textFile(file)
  val ds = sparkSession.read.text(file).as[String]

  println("Word count data set ...............")
  val wordsDS = ds.flatMap(value2 => value2.split(" "))
  val wordsPairDS = wordsDS.groupByKey(value2 =>value2)
  val wordCountDS = wordsPairDS.count
  wordCountDS.show()

}
