package spark1

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 20.3.18.
  */
object spark1 {
  def main(args: Array[String]): Unit = {
    val logfile = "/home/alex/scala/src/main/files/readme.log"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logfile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}