package spark2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alex on 21.3.18.
  */
object sparkTest3 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Spark test 3")
    val sparkContext = new SparkContext(sparkConf)

    println("Start sparkContext.textFile")
    val lines = sparkContext.textFile("/home/alex/spark/projects/spark1/src/main/scala/spark2/data1.txt")

    println("Start lines.map(line=>line.length) ")
    val linesLength = lines.map(line=>line.length)

    println("Persist  linesLength ")
    linesLength.persist()

    println("Calculated total length  - reduce")
    val totalLength = linesLength.reduce((a,b)=> a+b)
    println("Total length = " +  totalLength)

    sparkContext.stop()
  }

}
