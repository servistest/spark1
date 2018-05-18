package sparkDF

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 30.3.18.
  */
object sparkDF10 extends App{
  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  val peopleDF = spark.read.json("/home/alex/spark/projects/spark1/src/main/resources/people.json")

  peopleDF.show()
  peopleDF.printSchema()

  import spark.sqlContext._

  peopleDF.createOrReplaceTempView("people")


  peopleDF.sqlContext.sql("Select * from people").show()
  peopleDF.explain()

  val peopleDF2 = spark.sqlContext.sql("SELECT * FROM json.`/home/alex/spark/projects/spark1/src/main/resources/persons.json`")
  val peopleDF3 = spark.sqlContext.sql("SELECT * FROM parquet.`/home/alex/spark/projects/spark1/src/main/resources/users.parquet`")
  peopleDF2.show()
  peopleDF3.show()


}
