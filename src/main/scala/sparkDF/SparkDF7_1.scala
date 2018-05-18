package sparkDF



import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by alex on 30.3.18.
  */
object SparkDF7_1 extends App {

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val peopleRDD = spark.sparkContext.textFile("/home/alex/spark/projects/spark1/src/main/resources/person.txt")

  val peopleRow = peopleRDD
    .map(line => line.split(","))
    .map(attributes => Row((attributes(0).trim.toString, attributes(1).trim.toInt)))

  import org.apache.spark.sql.types._

  val schema = StructType(
    Array(StructField("name", StringType),
          StructField("age", IntegerType)
    )
  )

  val peopleDF = spark.createDataFrame(peopleRow, schema)
  //  peopleDF.show()

  spark.stop()
}
