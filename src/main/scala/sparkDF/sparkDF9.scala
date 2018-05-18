package sparkDF

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 30.3.18.
  */
object sparkDF9 extends App{

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  val peopleDF = spark.read.json("/home/alex/spark/projects/spark1/src/main/resources/people.json")

  peopleDF.show()
  peopleDF.printSchema()

  import spark.sqlContext.implicits._

  peopleDF.select("name").show()
  peopleDF.select($"name",peopleDF("name"),peopleDF.col("name")).show()
  peopleDF.select($"age" +1).show(2)
  peopleDF.filter($"age">20).show()
  peopleDF.groupBy("name").count().show()
  peopleDF.groupBy($"age").count().show()
  peopleDF.groupBy($"salary").sum().show()

  spark.stop()

}
