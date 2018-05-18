package spark2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alex on 22.3.18.
  */
object sparkTest8 extends App{

  val file = "/home/alex/spark/projects/spark1/src/main/scala/spark2/data2.txt"
  val sparkConf = new SparkConf().setAppName("Word count").setMaster("local[4]")
  val sparkContext = new SparkContext (sparkConf)

  val wordsRDD = sparkContext.textFile(file)

  val words = wordsRDD.flatMap(line => line.split(" "))

  words.foreach(x=> println(x))

  val sovpWord = words.map(x=>(x,1))
  sovpWord.foreach(println)

  val reduceWords = sovpWord.reduce((a1,b1) => (a1._1+b1._1,a1._2+b1._2))
  println("Reduce words  " )
  println(reduceWords)

  val reduceWordsByKey = sovpWord.reduceByKey((a,b) => a+b)
  println("Reduce words by Key  " )
  reduceWordsByKey.foreach(println)

  val countWords = wordsRDD
    .flatMap(line => line.split(" "))
  .map(x=> (x,1))
  .reduceByKey((a,b) => a+b)

  countWords.foreach(println)

  val countWord = wordsRDD.flatMap(line => line.split(" ")).count()
  println("Count of word in file = " + countWord)

  sparkContext.stop()



}
