package kafka

import org.apache.spark.sql.SparkSession
import sparkDF.sparkDF9.spark

/**
  * Created by alex on 12.4.18.
  */
object kafka1 extends App {


  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Kafka test")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("failOnDataLoss", "false")
    .option("subscribe", "testtopic2")
    //    .option("startingOffsets", "earliest")
    .load()


  val dfSelect1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  dfSelect1.printSchema()
  //  dfSelect1.createOrReplaceTempView("select1")
  //  val select1 = dfSelect1.sqlContext.sql("select * from  select1")


  val streamSelect1 = dfSelect1
    .writeStream
    .outputMode("update")
    .option("failOnDataLoss", "false")
    .format("console")
    .start()


  //  write to Kafka


  val ds = df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "testtopic2")
    .option("failOnDataLoss", "false")
    .option("checkpointLocation", "/home/alex/spark/projects/spark1/src/main/resources/stream1")
    .start()


  streamSelect1.awaitTermination()

}
