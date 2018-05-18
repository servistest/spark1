package book

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 27.4.18.
  */
object example3 extends App{

  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()
  val logRDD=  sparkSession.sparkContext.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log3.txt"))


  val result = logRDD.flatMap(line=> line.split(" ")).map(word => (word,1))
  println("Count of words = " + logRDD.flatMap(line => line.split(" ")).count())
  result.cache()
  result.reduceByKey((x,y) => (x+y)).foreach(println)

//  mkString(",")
//  result.count()

}
