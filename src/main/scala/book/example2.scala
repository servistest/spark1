package book


import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 26.4.18.
  */
object example2 extends App{

  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()
  val logRDD=  sparkSession.sparkContext.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log2.txt"))

  logRDD.map(println)
  val wordsRDD = logRDD.flatMap(lines => lines.split(" "))
  println(wordsRDD.first())


  import sparkSession.implicits._
  val logDS=  sparkSession.read.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log2.txt"))

  logDS.show(false)
   val wordsDS = logDS.map(lines => lines.split(" "))
  println(wordsDS.show(1,false))
  wordsDS.show(10,false)


  val input = sparkSession.sparkContext.parallelize(List(1, 2, 3, 4))
  val result = input.map(x => x * x)
  println(result.collect().mkString(","))


}
