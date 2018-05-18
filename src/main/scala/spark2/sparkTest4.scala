package spark2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alex on 21.3.18.
  */
object sparkTest4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Spark test 3")
    val sparkContext = new SparkContext(sparkConf)

    var counter=0
    val data = Array(0,1,2,3,4,5,6,7)
        val rdd =sparkContext.parallelize(data,4)
    println("Start foreach ...")
    rdd.foreach(x=> counter += x)

    // Wrong: Don't do this!!
    println("Counter = "+counter)


//    sparkContext.stop()

  }

}
