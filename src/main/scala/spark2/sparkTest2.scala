package spark2

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by alex on 21.3.18.
  */
object sparkTest2 {

  def main(args: Array[String]): Unit = {
//    The master URL to connect to, such as "local" to run locally with one thread,
// "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
    val conf = new SparkConf().setAppName("First Program").setMaster("local")
    val sparkContext = new SparkContext(conf)
    println("I started my first program")

    //----------------------------------------------------------
    val data = Array(1, 2, 3, 4, 5)
    val distData = sparkContext.parallelize(data,4)
    println(distData.getClass)
    println(distData.reduce((a,b) =>a+b))
//    ----------------------------------------------------------

    val datafile = "/home/alex/spark/projects/spark1/src/main/scala/spark2/data1.txt"
    val textFileRDD = sparkContext.textFile(datafile)

    val textFileReduce = textFileRDD.cache().reduce((str1,str2) => str1 concat str2)


    val lengthTextFile = textFileRDD.map(s=>s.length).reduce((a,b)=> a+b)
    println("length of text file = " + lengthTextFile)
    val listOfFiles =sparkContext.wholeTextFiles("/home/alex/spark/projects/spark1/src/main/scala/spark2/")
    listOfFiles.foreach(item => println(item._1)  )
    listOfFiles.foreach(item => println(item._2)  )

//    textFileRDD.saveAsObjectFile("/home/alex/spark/projects/spark1/src/main/scala/spark2/saveObjects.txt")
//      textFileRDD.saveAsTextFile("/home/alex/spark/projects/spark1/src/main/scala/spark2/saveTexts.txt")
    sparkContext.stop()


  }
}
