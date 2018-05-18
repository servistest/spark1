package book

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 25.4.18.
  */
object example1_DS extends App{

      val spark = SparkSession.builder()
      .appName("Analazing logs")
      .master("local[2]")
      .getOrCreate()


    val logDS=  spark.read.textFile("/home/alex/spark/projects/spark1/src/main/resources/logs/log1.txt")

    val errorDS = logDS.filter(line => line.contains("ERROR"))
    errorDS.show(false)

    val warnDS = logDS.filter(line => line.contains("WARN"))
    warnDS.show(false)

    println("Print union ")
    errorDS.union(warnDS).show(false)

    spark.stop()




}
