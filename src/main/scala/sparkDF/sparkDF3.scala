package sparkDF


import org.apache.spark.sql.SparkSession
/**
  * Created by alex on 29.3.18.
  */
object sparkDF3 extends App{

  case class Person(name: String, age: Long)

val spark = SparkSession
    .builder()
    .appName("Data set test")
    .master("local[2]")
    .getOrCreate()



  import spark.implicits._
  val caseClassDS = Seq(Person("Alex",45)).toDS()

  caseClassDS.show()


  val primitiveDS = Seq(0,1,2,3).toDS()
  println(primitiveDS.map(_ + 1).collect().deep)


  val path = "/home/alex/spark/projects/spark1/src/main/resources/persons.json"
  val peopleDS = spark.read.json(path).as[Person]
  peopleDS.show()







}
