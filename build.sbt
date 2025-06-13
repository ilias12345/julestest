name := "spark-kafka-consumers"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15" // Updated to a more recent 2.12 patch

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "3.0.0", // A recent Kafka client version
  "org.scalatest" %% "scalatest" % "3.2.10" % Test // ScalaTest for testing
)
