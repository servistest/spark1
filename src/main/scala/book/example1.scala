package book

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 25.4.18.
  */
object example1  extends App{

  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()

// val logDF=  sparkSession.read.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log1.txt"))

  val logDF=  sparkSession.sparkContext.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log1.txt"))


  val errorRDD = logDF
    .filter(line => line.contains("ERROR"))
  errorRDD.foreach(println)


  val warningRDD = logDF
    .filter(line => line.contains("WARN"))
  warningRDD.foreach(println)

  println("Print union ")
  errorRDD.union(warningRDD).foreach(println)

  sparkSession.stop()



}
