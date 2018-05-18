name := "scala"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.3.0" exclude("org.apache.spark", "spark-sql"),
  "org.apache.hadoop" % "hadoop-client" % "2.9.0" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0" exclude("org.apache.spark", "spark-sql-kafka-0-10"),
  "org.apache.spark" %% "spark-streaming" % "2.3.0"
)
//spark-streaming_2.11-2.3.0.jar