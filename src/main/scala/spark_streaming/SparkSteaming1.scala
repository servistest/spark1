package spark_streaming


import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

/**
  * Created by alex on 14.4.18.
  */
object SparkSteaming1 extends App{

  val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(1))


}
