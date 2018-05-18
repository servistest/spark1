package book

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 3.5.18.
  */
object example7 extends App{


  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()



  val rdd = sparkSession.sparkContext.parallelize(Array ((1, 2), (3, 4), (3, 6)))

  rdd.foreach(println)

  rdd.countByKey().foreach(println)
  val rdd2 = rdd.collectAsMap()

  println("rdd.partitioner")
  println(rdd.partitioner.getOrElse())

  val partitioned = rdd.partitionBy(new HashPartitioner(2))
  println(partitioned.partitioner.getOrElse())









}
