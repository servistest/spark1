package sparkDF

import org.apache.spark.sql.SparkSession
import sparkDF.sparkDF5.spark

/**
  * Created by alex on 29.3.18.
  */
object sparkDF6 extends App{

  val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val usersDF = spark.read.load("/home/alex/spark/projects/spark1/src/main/resources/users.parquet")
  usersDF.cache()
  usersDF.select("name","favorite_color").write.save("namesAndFavColors.parquet")







}
