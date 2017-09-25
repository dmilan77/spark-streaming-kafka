name := "spark-streaming-kafka"

version := "1.0"

scalaVersion := "2.10.6"
val sparkVersion = "1.6.0"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.25" % "test",
  "org.apache.spark" %% "spark-streaming" %sparkVersion,
  //"org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-core"  %sparkVersion,
  "org.apache.spark" %% "spark-sql" %sparkVersion
)

        