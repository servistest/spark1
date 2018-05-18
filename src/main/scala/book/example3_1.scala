package book

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by alex on 27.4.18.
  */
object example3_1 extends App{

  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()

  import sparkSession.implicits._
  val input = sparkSession.sparkContext.parallelize(Array(0,2,3,45))

  val result = input.map(x => x * x)
  result.persist(StorageLevel.MEMORY_ONLY)
  println(result.count())
  println(result.collect().mkString(","))

}
