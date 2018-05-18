package book

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 25.4.18.
  */
object example1_DF extends  App {

  val spark = SparkSession.builder()
    .appName("Analazing logs")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val logDF = spark.read.text("/home/alex/spark/projects/spark1/src/main/resources/logs/log1.txt")
  logDF.show(false)
  logDF.printSchema()
  logDF.filter(_!= null)

      println("Print filter ")
  val logERROR = logDF.filter("value like '%ERROR%'").show(100,false)
  val logWARN = logDF.filter("value like '%WARN%'").show(100,false)



  spark.stop()


}
