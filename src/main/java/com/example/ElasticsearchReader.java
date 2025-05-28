package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElasticsearchReader {

    private static final Logger logger = LogManager.getLogger(ElasticsearchReader.class);

    private SparkSession sparkSession;

    public ElasticsearchReader(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        // ES connection properties (es.nodes, es.port, es.scheme, es.nodes.wan.only)
        // are now expected to be set on the SparkSession's configuration
        // *before* this reader is instantiated.
        // See SparkElasticsearchKafkaApp where these are set.
    }

    public Dataset<Row> readData(String indexName, String query) {
        logger.info("Reading data from Elasticsearch index: {} with query: {}", indexName, query);
        try {
            // Use EsSparkSQL to read data from Elasticsearch
            // The 'query' parameter here is the ES query string (e.g., "?q=user:kimchy")
            // EsSparkSQL.esDF(sparkSession, resource, query)
            // resource is typically "index/type" or just "index" for ES 6+
            // The query parameter in esDF is for Lucene query syntax passed as a query string parameter.
            // For more complex JSON queries, they are often passed as part of the options map.
            // Example: Map<String, String> options = new HashMap<>(); options.put("es.query", "{ \"match_all\": {} }");
            // For this implementation, we assume the query string passed to readData is sufficient.

            Dataset<Row> data = EsSparkSQL.esDF(sparkSession, indexName, query);
            logger.info("Successfully read data from Elasticsearch index: {}", indexName);
            return data;
        } catch (Exception e) {
            logger.error("Error reading data from Elasticsearch index: {} with query: {}", indexName, query, e);
            return sparkSession.emptyDataFrame();
        }
    }

    // Example usage:
    // public static void main(String[] args) {
    //     SparkSession spark = SparkSession.builder()
    //             .appName("ElasticsearchReaderExample")
    //             .master("local[*]")
    //             .config("es.nodes", "localhost") // Example configuration
    //             .config("es.port", "9200")
    //             .config("es.scheme", "http")
    //             .config("es.nodes.wan.only", "true")
    //             .getOrCreate();

    //     ElasticsearchReader reader = new ElasticsearchReader(spark);

    //     // Example: Read all documents from an index
    //     Dataset<Row> allData = reader.readData("your_index_name", "?q=*");
    //     allData.show();

    //     // Example: Read specific documents using a Lucene query
    //     Dataset<Row> specificData = reader.readData("your_index_name", "?q=some_field:some_value");
    //     specificData.show();

    //     spark.stop();
    // }
}
