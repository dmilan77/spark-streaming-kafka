name := "spark-streaming-kafka"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.25" % "test",
  "org.apache.spark" % "spark-streaming_2.10" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3"
)

        