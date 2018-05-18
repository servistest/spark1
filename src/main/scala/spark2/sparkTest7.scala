package spark2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alex on 22.3.18.
  */
object sparkTest7 extends App{

  val file = "/home/alex/spark/projects/spark1/src/main/scala/spark2/data2.txt"
  val fileResult = "/home/alex/spark/projects/spark1/src/main/scala/spark2/data2result.txt"
  val sparkConf = new SparkConf().setAppName("Word count").setMaster("local[4]")
  val sparkContext = new SparkContext (sparkConf)

  val wordsRDD= sparkContext.textFile(file).map(line => line.split(""))
  wordsRDD.foreach(x=> print(x.deep))

  val wordsRDDFlatMap = sparkContext.textFile(file).flatMap(line => line.split(" "))
    wordsRDDFlatMap.foreach(println)

  val mapWords = wordsRDDFlatMap.map(x=> (x,1))
  mapWords.foreach(println)

  var reduceWordRDD = mapWords.reduceByKey(_ + _)
  println("Count  of words in file = " )
  reduceWordRDD.foreach(println)


  val countOfWords =mapWords.count()
  println("Count  of words in file = " + countOfWords)

  val counts= sparkContext.textFile(file)
    .flatMap(line => line.split(" "))
      .map(word=>(word,1))
    .reduceByKey( (a,b) => a+b )
  println("Count  of words in file = " + counts)
//  counts.saveAsTextFile(fileResult)






//    .map(word=> (word,1))
//    .reduceByKey(_ + _)
//  print("Count  od words in file = " + count)


}
