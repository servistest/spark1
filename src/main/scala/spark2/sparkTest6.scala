package spark2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alex on 21.3.18.
  */
object sparkTest6 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Spark test 6")
    val sparkContext = new SparkContext(sparkConf)


    val broadcastVar = sparkContext.broadcast(Array(0,1,2,3,4,5))
    broadcastVar.value.foreach(print)


    sparkContext.stop()
  }


}
