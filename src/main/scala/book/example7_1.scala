package book

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 3.5.18.
  */
object example7_1 extends App {


  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()


  val userData = sparkSession.sparkContext.parallelize(Array((1, "Facebook"), (2, "Andy"), (3, "Justin")))
  val userLink = sparkSession.sparkContext.parallelize(Array((1, "Facebook"), (1, "Vk"), (1, "Wiki"), (2, "Facebook"),(3, "Justin2")))

  val joinedRow = userData.join(userLink)
  joinedRow.foreach(println)

  val joinedRowFiltered = joinedRow.filter(x => x._2._2.contains("Facebook"))

  println("joinedRowFiltered.foreach(println)")
  joinedRowFiltered.foreach(println)

  val joinedRowFiltered2 = joinedRow.filter {

    case (userId, (userInfo, clickInfo)) => !clickInfo.equals(userInfo)
  }

  println("joinedRowFiltered2.foreach(println)")
  joinedRowFiltered2.foreach(println)




}
