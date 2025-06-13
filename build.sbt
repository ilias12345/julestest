name := "spark-kafka-consumers-java" // Changed name slightly
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.15" // Keep for Spark compatibility

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
  "junit" % "junit" % "4.13.2" % Test // Added JUnit
)

// Ensure sbt knows where to find Java sources if not default src/main/java
// However, sbt defaults to src/main/java for Java sources.
// unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/java"
// unmanagedSourceDirectories in Test += baseDirectory.value / "src/test/java"
// These lines are usually not needed if using standard directory structure.
