package streaming

import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by alex on 3.4.18.
  */
object streaming_1 extends App{

  val spark =SparkSession.
    builder
      .master("local[2]")
    .appName("Word counts stream")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port",9999)
    .load()


  val words = lines.as[String].flatMap(line => line.split(" "))


  val countOfWords = words.groupBy("value").count()

  val query = countOfWords.writeStream
    .outputMode("complete")
//      .outputMode("update")

    .format("console")
    .start()

  query.awaitTermination()


}
