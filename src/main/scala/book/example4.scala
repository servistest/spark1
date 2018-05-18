package book

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 27.4.18.
  */
object example4 extends App{

  val sparkSession = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()
  val logRDD=  sparkSession.sparkContext.textFile(("/home/alex/spark/projects/spark1/src/main/resources/logs/log3.txt"))



  val result = logRDD.map(line => (line.split(" ")(0),line))
  result.foreach(println)


  val myRDD = sparkSession.sparkContext.parallelize(Array ((1, 2), (3, 4), (3, 6)))
  myRDD.cache()
  println("1 ---------")
  myRDD.foreach(println)

  println("1.1 --------- myRDD.groupByKey()")
  myRDD.groupByKey().foreach(println)

  println("1 --------- myRDD.reduceByKey((x,y) => x+y).foreach(println)")
  myRDD.reduceByKey((x,y) => x+y).foreach(println)

  val pairsRDD2 = sparkSession.sparkContext.parallelize(Seq((1, 2), (3, 4), (3, 6)))

//  val  = myRDD2.map(x=> (x,1))
  pairsRDD2.cache()
  println("2 ---------   pairsRDD2 = sparkSession.sparkContext.parallelize(Seq((1, 2), (3, 4), (3, 6))")
  pairsRDD2.cache().foreach(println)


  println("2 ---------   pairsRDD2.reduceByKey((x,y) => x+y) ")
  pairsRDD2.reduceByKey((x,y) => x+y).foreach(println)

  println("2 ---------   pairsRDD2.groupByKey()")
  pairsRDD2.groupByKey().foreach(println)

  println("2 ---------   pairsRDD2.mapValues(x=>x+1)")
  pairsRDD2.mapValues(x=>x+1).collect().foreach(println)

  println("2 ---pairsRDD2.flatMapValues(x=>x to 7) ")
  pairsRDD2.flatMapValues(x=>x to 7).foreach(println)

  println("2 ---pairsRDD2.keys")
  pairsRDD2.keys.foreach(println)
//  result.reduceByKey((x,y) => (x+y)).foreach(println)

  println("2 ---pairsRDD2.sortByKey()")
  pairsRDD2.sortByKey().foreach(println)


  val otherRDD = sparkSession.sparkContext.parallelize(Seq((3,9)))
  otherRDD.cache()

  println("2 ---pairsRDD2.subtractByKey ( (3,9) )")
  pairsRDD2.subtractByKey(otherRDD).foreach(println)

  println("2 ---pairsRDD2.join(otherRDD)")
  pairsRDD2.join(otherRDD).foreach(println)

  println("2 ---pairsRDD2.rightOuterJoin(otherRDD)")
  pairsRDD2.rightOuterJoin(otherRDD).foreach(println)


  println("2 ---pairsRDD2.cogroup(otherRDD)")
  pairsRDD2.cogroup(otherRDD).foreach(println)


  val listTuples= ((1,"1","First",1.0),(2,"2","Second",2.0))
  val iterable = Iterable((1,"1","First",1.0),(2,"2","Second",2.0))

  val iterableRDD= sparkSession.sparkContext.parallelize(iterable.toList).cache()


  println("--------------Iterable RDD print -----------------")
  iterableRDD.foreach(println)



  println("--------------iterableRDD.map(x => (x._1 +x._4,x._2 + x._3)).foreach(println)  -----------------")
//  println(iterable.mkString(","))
  iterableRDD.map(x => (x._1 +x._4,x._2 +"."+ x._3)).foreach(println)
//    .sortByKey().foreach(println)





}
