package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SparkConsumer2App {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
            .appName("SparkKafkaConsumer2Java") // Changed app name
            .master("local[*]")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("kafka.bootstrap.servers", "localhost:9092");
        kafkaParams.put("subscribe", "test-topic");
        kafkaParams.put("startingOffsets", "earliest");
        kafkaParams.put("group.id", "consumer-group-2"); // Changed group.id

        Dataset<Row> df = spark.readStream()
            .format("kafka")
            .options(kafkaParams)
            .load();

        Dataset<Row> messages = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        StreamingQuery query = messages.writeStream()
            .outputMode("append")
            .format("console")
            .start();

        query.awaitTermination();
    }
}
