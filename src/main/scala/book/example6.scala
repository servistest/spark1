package book

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 3.5.18.
  */
object example6 extends App{


  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()


  val rdd = sparkSession.sparkContext.parallelize(Seq("panda" -> 0 ,"pink" -> 3,"pirate" -> 3,"panda" -> 1,"pink" -> 4))

  rdd.foreach(println)
  println("rdd.groupByKey().foreach(println)")
  rdd.groupByKey().foreach(println)

  val rdd2 = rdd.mapValues( x => (x,1))
  rdd2.foreach(println)

  println("rdd2.reduceByKey((x,y) => (x._1+y._1, x._2 + y._2))")

  rdd2.reduceByKey((x,y) => (x._1+y._1, x._2 + y._2)).foreach(println)

  println("rdd.sortByKey().foreach(println)")
  rdd.sortByKey().foreach(println)


}
