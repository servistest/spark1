package spark2


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Created by alex on 22.3.18.
  */
object sparkTest9 extends App{

  val  file = "/home/alex/spark/projects/spark1/src/main/scala/spark2/data3.txt"

  val sparkSession = SparkSession.builder().master("local[4]").appName("spark session Data Set ").getOrCreate()
  val sparkContext = sparkSession.sparkContext

  import sparkSession.implicits._
  val rdd = sparkContext.textFile(file)
  val ds = sparkSession.read.text(file).as[String]

  println("Print df.......................... ")
  ds.rdd.foreach(println)
  println("Print rdd.......................... ")
  rdd.foreach(println)

  println("count...............")
  println(rdd.count())
  println(ds.count())

  println("Word count rdd...............")
  val wordsRDD = rdd.flatMap(line => line.split(" "))
  val wordsPairRDD = wordsRDD.map(word => (word,1))
  val wordCountRDD = wordsPairRDD.reduceByKey((a,b) => a+b)
  wordCountRDD.foreach(println)
  println("Word count COLLECT rdd...............")
  println(wordCountRDD.collect().toList)


  println("Word count data set ...............")
  val wordsDS = ds.flatMap(value => value.split(" "))
  val wordsPairDS = wordsDS.groupByKey(value =>value)
  val wordCountDS = wordsPairDS.count
  wordCountDS.show()

  rdd.cache()
  ds.cache()

  //filter

  val filterRDD = wordsRDD.filter(value => value == "INFO" )
  println(" filter RDD = " + filterRDD.collect().toList)

  val filterDS = wordsDS.filter(value=>value=="INFO")
  filterDS.show()
  val filterDS2 = ds.filter(line=>line.contains( "ERROR"))
  filterDS2.show()


  //converting to each other
  val dsToRDD = ds.rdd
  println("ds To RDD...............")
  dsToRDD.foreach(println)
  println("ds To RDD  and collect...............")
  dsToRDD.collect().foreach(println)


  // double based operation

  val doubleRDD = sparkContext.makeRDD(List(0.0,1.0,2.6,3.7,4.4,5.7))


  doubleRDD.foreach(println)
  println(doubleRDD.sum())
  println(doubleRDD.mean())


  val rowRDD = doubleRDD.map(value => Row.fromSeq(List(value)))
  println(Row.fromSeq(List(0, 1,2)))

  rowRDD.foreach(println)

  val rowRDD2 = Array(0,1,2,3,4,5,6,7) map(value => Row.fromSeq(List(value)))
  rowRDD2.foreach(println)
  println(rowRDD2.getClass)


  val schema = StructType(Array(StructField("value",DoubleType)))

  val doubleDS = sparkSession.createDataFrame(rowRDD,schema)

  import org.apache.spark.sql.functions._

  doubleDS.agg(sum("value")).show()
  doubleDS.agg(mean("value")).show()

  val reduceCountByRDD= wordsPairRDD.reduceByKey(_ + _)
  val reduceCountByDS = wordsPairDS.mapGroups((key,value) => (key,value.length))


  println(reduceCountByRDD.collect().toList)
  println(reduceCountByDS.collect().toList)


  val rddReduce = doubleRDD.reduce((a,b) => a +b)
  val dsReduce = doubleDS.reduce((row1,row2) =>Row(row1.getDouble(0) + row2.getDouble(0)))
}
