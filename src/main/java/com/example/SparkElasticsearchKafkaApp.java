package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.example.config.AppConfig;

public class SparkElasticsearchKafkaApp {

    private static final Logger logger = LogManager.getLogger(SparkElasticsearchKafkaApp.class);

    public static void main(String[] args) {
        logger.info("Starting Elasticsearch to Kafka Application");

        // Load configurations
        AppConfig esConfig = null;
        AppConfig kafkaConfig = null;
        try {
            esConfig = new AppConfig("elasticsearch.properties");
            kafkaConfig = new AppConfig("kafka.properties");
            logger.info("Configurations loaded successfully.");
        } catch (RuntimeException e) {
            logger.fatal("Failed to load configuration. Application will exit.", e);
            return;
        }

        String esHostname = esConfig.getEsNodes();
        int esPort = esConfig.getEsPort();
        String esScheme = esConfig.getEsScheme();
        String esIndex = esConfig.getEsIndex();
        String esQuery = esConfig.getEsQuery();

        String kafkaBootstrapServers = kafkaConfig.getKafkaBootstrapServers();
        String kafkaTopic = kafkaConfig.getKafkaTopic();

        SparkSession spark = null;
        KafkaDataProducer kafkaProducer = null;

        try {
            // 1. Initialize SparkSession
            logger.info("Initializing SparkSession...");
            SparkSession.Builder sparkBuilder = SparkSession.builder()
                    .appName("ElasticsearchToKafka")
                    .master("local[*]"); // Use local master for development/testing

            // Configure Elasticsearch connection for Spark
            // These must be set before the SparkSession is created if using certain ES connectors,
            // or on the session's runtime config if the connector supports it.
            // For es-hadoop, setting them on the SparkConf which is part of the session is typical.
            sparkBuilder.config("es.nodes", esHostname);
            sparkBuilder.config("es.port", String.valueOf(esPort));
            sparkBuilder.config("es.scheme", esScheme);
            sparkBuilder.config("es.nodes.wan.only", "true"); // Example: In case of WAN access

            spark = sparkBuilder.getOrCreate();
            logger.info("SparkSession initialized successfully.");

            // 2. Instantiate ElasticsearchReader
            // ElasticsearchReader constructor no longer needs to set these in SparkConf,
            // as we are doing it above before creating the session.
            // It will just use the passed SparkSession.
            logger.info("Initializing ElasticsearchReader with ES settings: host={}, port={}, scheme={}", esHostname, esPort, esScheme);
            ElasticsearchReader esReader = new ElasticsearchReader(spark); // Pass only SparkSession

            // 3. Read data from Elasticsearch
            logger.info("Reading data from Elasticsearch index: {} with query: {}", esIndex, esQuery);
            Dataset<Row> data = esReader.readData(esIndex, esQuery); // Pass index and query

            if (data == null || data.isEmpty()) { // data.isEmpty() is a costly operation for large datasets, consider data.rdd().isEmpty() or other checks
                logger.warn("No data read from Elasticsearch index: {}. Exiting application.", esIndex);
                return; // Exit if no data
            }
            logger.info("Successfully read {} rows from Elasticsearch.", data.count());
            data.show(5, false); // Show a few rows for debugging

            // 4. Instantiate KafkaDataProducer
            logger.info("Initializing KafkaDataProducer with bootstrap servers: {}", kafkaBootstrapServers);
            kafkaProducer = new KafkaDataProducer(kafkaBootstrapServers);

            // 5. Send data to Kafka
            logger.info("Sending data to Kafka topic: {}", kafkaTopic);
            kafkaProducer.sendData(data, kafkaTopic);
            logger.info("Data sending to Kafka topic {} initiated.", kafkaTopic);

        } catch (Exception e) {
            logger.error("An error occurred during the Elasticsearch to Kafka process.", e);
            // Depending on the severity, you might want to exit or perform other cleanup
        } finally {
            // 6. Close KafkaProducer
            if (kafkaProducer != null) {
                logger.info("Closing KafkaDataProducer...");
                try {
                    kafkaProducer.close();
                    logger.info("KafkaDataProducer closed successfully.");
                } catch (Exception e) {
                    logger.error("Error closing KafkaDataProducer.", e);
                }
            }

            // 7. Stop SparkSession
            if (spark != null) {
                logger.info("Stopping SparkSession...");
                try {
                    spark.stop();
                    logger.info("SparkSession stopped successfully.");
                } catch (Exception e) {
                    logger.error("Error stopping SparkSession.", e);
                }
            }
            logger.info("Elasticsearch to Kafka Application finished.");
        }
    }
}
