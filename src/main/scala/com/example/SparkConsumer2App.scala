package com.example

import org.apache.spark.sql.SparkSession

object SparkConsumer2App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkKafkaConsumer2") // Changed app name
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> "localhost:9092",
      "subscribe" -> "test-topic",
      "startingOffsets" -> "earliest",
      "group.id" -> "consumer-group-2" // Changed group.id
    )

    val df = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
