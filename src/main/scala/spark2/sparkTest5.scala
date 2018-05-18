package spark2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alex on 21.3.18.
  */
object sparkTest5 {

  def main(args: Array[String]): Unit = {
    val file = "/home/alex/spark/projects/spark1/src/main/scala/spark2/data2.txt"
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Spark test 4")
    val sparkContext = new SparkContext(sparkConf)
    println("spark Conf = " +  sparkConf.getAll.deep)
    println("spark Context = " +  sparkContext.getAllPools)

    val lines = sparkContext.textFile(file)
    val pairs = lines.map(x => (x, 1))
    pairs.foreach(println)

    val counts = pairs.reduceByKey((a,b) => a+b)
    counts.foreach(println)
    println("Println collect = ")
    println(counts.collect().deep)
    println(Array(0, 1, 2, 3,3).distinct.deep)

    println("Println cartesian = ")
    pairs.cartesian(lines).foreach(println)

    val countByKey= pairs.countByKey()
    println("CountByKey =" + countByKey)


    sparkContext.stop()

  }


}
