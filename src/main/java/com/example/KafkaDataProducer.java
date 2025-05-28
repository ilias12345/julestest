package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class KafkaDataProducer {

    private static final Logger logger = LogManager.getLogger(KafkaDataProducer.class);
    private KafkaProducer<String, String> producer;
    private String bootstrapServers;

    public KafkaDataProducer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Add other producer configurations as needed, e.g., acks, retries

        try {
            this.producer = new KafkaProducer<>(props);
            logger.info("KafkaProducer initialized successfully for bootstrap servers: {}", bootstrapServers);
        } catch (Exception e) {
            logger.error("Error initializing KafkaProducer for bootstrap servers: {}", bootstrapServers, e);
            // Depending on the application's needs, you might want to throw a custom exception here
            // or handle it in a way that the application can gracefully degrade.
            // For now, the producer will be null, and sendData will fail.
        }
    }

    public void sendData(Dataset<Row> data, String topicName) {
        if (producer == null) {
            logger.error("KafkaProducer is not initialized. Cannot send data to topic: {}", topicName);
            return;
        }

        logger.info("Starting to send data to Kafka topic: {}", topicName);
        try {
            // Iterate over the Dataset and send each row as a JSON string
            // Using foreachPartition to manage resources effectively if the Dataset is large
            data.foreachPartition(partition -> {
                if (producer == null) { // Check again in case it failed initialization but was not caught by the initial check
                    logger.error("KafkaProducer is not initialized within partition. Cannot send data.");
                    return; // or throw new RuntimeException("KafkaProducer not initialized");
                }
                partition.forEachRemaining(row -> {
                    try {
                        String jsonMessage = row.json();
                        // Using null as key, messages will be distributed round-robin across partitions
                        // If a key is needed, it can be derived from the row
                        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, jsonMessage);
                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                logger.debug("Message sent successfully to topic: {}, partition: {}, offset: {}",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            } else {
                                logger.error("Error sending message to Kafka topic: {}", topicName, exception);
                            }
                        });
                    } catch (Exception e) {
                        logger.error("Error processing row or sending message to Kafka topic: {}", topicName, e);
                    }
                });
            });
            logger.info("Finished sending data to Kafka topic: {}", topicName);
        } catch (Exception e) {
            logger.error("Error sending data to Kafka topic: {}", topicName, e);
            // Consider re-throwing or specific error handling based on requirements
        }
        // Note: producer.flush() might be needed here if you want to ensure all messages are sent
        // before the method returns, especially in a non-Spark context or at the end of the application.
        // However, in a Spark streaming context, this might be handled differently.
        // For batch Dataset, flushing here might be appropriate.
        // producer.flush();
    }

    public void close() {
        if (producer != null) {
            try {
                producer.flush(); // Ensure all outstanding messages are sent
                producer.close();
                logger.info("KafkaProducer closed successfully.");
            } catch (Exception e) {
                logger.error("Error closing KafkaProducer.", e);
            }
        }
    }

    // Example usage:
    // public static void main(String[] args) {
    //     SparkSession spark = SparkSession.builder()
    //             .appName("KafkaDataProducerExample")
    //             .master("local[*]")
    //             .getOrCreate();

    //     // Create a sample Dataset<Row>
    //     List<String> data = Arrays.asList("{\"name\":\"alice\",\"age\":30}", "{\"name\":\"bob\",\"age\":25}");
    //     Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    //     Dataset<Row> df = spark.read().json(ds);
    //     df.show();

    //     // Replace with your Kafka broker details
    //     KafkaDataProducer dataProducer = new KafkaDataProducer("localhost:9092");
    //     dataProducer.sendData(df, "my_topic");
    //     dataProducer.close();

    //     spark.stop();
    // }
}
