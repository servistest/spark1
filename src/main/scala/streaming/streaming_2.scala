package streaming

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  * Created by alex on 3.4.18.
  */
object streaming_2 extends App {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Streaming from file ")
    .getOrCreate()

  import spark.implicits._
  val socketDF = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  socketDF.isStreaming
  socketDF.printSchema()

  val userSchema = new StructType()
    .add("name", "string")
    .add("age", "integer")

  val csvData = spark
    .readStream
    .option("sep",";")
    .schema(userSchema)
      .csv("/home/alex/spark/projects/spark1/src/main/resources/stream1")

  csvData.printSchema()

//  val testCsvData = csvData.map(line => line +" tets").
//    writeStream
//        .outputMode("complete")
//      .format("console")
//    .start()
//  testCsvData.foreach(line => println(line))


}
