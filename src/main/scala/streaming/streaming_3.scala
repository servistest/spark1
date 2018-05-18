package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.types.StructType


/**
  * Created by alex on 10.4.18.
  */
object streaming_3 extends App {

  case class DeviceData(device: String, deviceType: String, signal: Double)

  val spark = SparkSession
    .builder()
    .appName("Stream devices")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val schema = new StructType().add("device", "string").add("deviceType", "string").add("signal", "double")

  val df = spark
    .readStream
    .schema(schema)
    .option("sep", ",")
    .csv("/home/alex/spark/projects/spark1/src/main/resources/stream2")

  spark.sparkContext.setLogLevel("ERROR")


  //  df.printSchema()

  println("df - query")
  val df_devices = df.select("device").where("signal>10")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()


  val df_group = df.groupBy("deviceType").count()
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()


  val ds = df.as[DeviceData]
  //  val ds_devices= ds.filter("signal >30").map(_.device)
  println("ds - devices")
  val ds_devices = ds.filter("signal >30")
    .writeStream
    .outputMode("append")
    .format("console")
    .start()


  val ds_goup = ds.groupBy("signal").count()
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  val dsGroupByKey = ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  val dsGroupByKey2 = ds.groupByKey(_.deviceType).agg(typed.count(_.deviceType))
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  df_devices.awaitTermination()


}
