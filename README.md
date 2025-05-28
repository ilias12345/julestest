# Spark Elasticsearch Kafka Connector

This project demonstrates how to read data from Elasticsearch using Apache Spark and send it to an Apache Kafka topic.

## Prerequisites

*   Java JDK (e.g., 1.8 or 11)
*   Apache Maven
*   Access to an Elasticsearch instance
*   Access to a Kafka broker

## Configuration

Configuration for Elasticsearch and Kafka is managed in property files located in `src/main/resources/`:

1.  **Elasticsearch (`elasticsearch.properties`)**:
    *   `es.nodes`: Comma-separated list of Elasticsearch nodes (e.g., `localhost`).
    *   `es.port`: Elasticsearch HTTP port (e.g., `9200`).
    *   `es.scheme`: Connection scheme (`http` or `https`).
    *   `es.index`: The Elasticsearch index to read from.
    *   `es.query`: The query to select data (e.g., `?q=*:*` for all documents).
    *   `es.nodes.wan.only`: (Optional) Set to `true` if connecting to a WAN cluster. Defaults to `false`.

2.  **Kafka (`kafka.properties`)**:
    *   `kafka.bootstrap.servers`: Comma-separated list of Kafka brokers (e.g., `localhost:9092`).
    *   `kafka.topic`: The Kafka topic to which data will be sent.

3.  **Logging (`log4j2.properties`)**:
    *   Configure logging levels and appenders as needed. Default is INFO to console.

## Building the Project

1.  Clone the repository.
2.  Navigate to the project root directory.
3.  Build the project using Maven:
    ```bash
    mvn clean package
    ```
    This will create a JAR file in the `target/` directory (e.g., `spark-elasticsearch-kafka-1.0-SNAPSHOT.jar`).

## Running the Application

1.  Ensure your Elasticsearch and Kafka instances are running and accessible.
2.  Update the configuration files in `src/main/resources/` as per your environment. If you are running outside an IDE, you might need to ensure these files are on the classpath or repackage the JAR with updated files. For running with `spark-submit`, you can package these into the JAR.
3.  Submit the application to Spark:
    ```bash
    spark-submit \
      --class com.example.SparkElasticsearchKafkaApp \
      --master <your_spark_master> \  # e.g., local[*] or yarn
      target/spark-elasticsearch-kafka-1.0-SNAPSHOT.jar
    ```
    Replace `<your_spark_master>` with your Spark master URL.

    **Note on Dependencies for `spark-submit`**:
    The `pom.xml` includes common Spark, Elasticsearch, and Kafka dependencies.
    When using `spark-submit`, ensure that compatible versions of these libraries are available on your Spark cluster's classpath, or consider building an uber JAR (fat JAR) that includes these dependencies. The current `pom.xml` does not build an uber JAR by default. You might need to adjust your `spark-submit` command to include packages or JARs:
    ```bash
    spark-submit \
      --class com.example.SparkElasticsearchKafkaApp \
      --master <your_spark_master> \
      --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.17.7,org.apache.kafka:kafka-clients:3.5.1 \ # Adjust versions as per pom.xml
      target/spark-elasticsearch-kafka-1.0-SNAPSHOT.jar
    ```
    Alternatively, to build an uber JAR, you can add the `maven-shade-plugin` to your `pom.xml`.

## Running with Docker Compose

This section describes how to run the entire application stack (Elasticsearch, Zookeeper, Kafka, and the Spark application) using Docker Compose.

### Prerequisites

*   Docker Desktop (or Docker Engine + Docker Compose CLI) installed.

### Overview

The `docker-compose.yml` file in the project root is configured to:
1.  Start Elasticsearch, Zookeeper, and Kafka services in separate containers.
2.  Build a Docker image for the Spark application using the provided `Dockerfile`.
3.  Run the Spark application container, connecting it to the other services within a dedicated Docker network.

### Configuration in Docker Environment

*   The `docker-compose.yml` file sets crucial environment variables for the `spark-app` service, such as:
    *   `ES_NODES=elasticsearch`
    *   `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`
    These environment variables are used by `AppConfig.java` to override values from the `.properties` files, enabling correct service discovery within Docker's network (e.g., `elasticsearch` refers to the Elasticsearch service container).
*   The base configuration files from `src/main/resources/` (e.g., `elasticsearch.properties`, `kafka.properties`) are still used for settings not overridden by environment variables. They are copied into the Spark application's Docker image during the `docker build` process.

### Building and Running

1.  **Build the application JAR**:
    Before building the Docker image, ensure your project's JAR file is created:
    ```bash
    mvn clean package
    ```

2.  **Start services with Docker Compose**:
    Navigate to the project root directory and run:
    ```bash
    docker-compose up --build
    ```
    *   The `--build` flag tells Docker Compose to build the Spark application image (as defined in `Dockerfile`) before starting the services. This is necessary the first time you run the command or if you've made changes to the `Dockerfile`, application source code, or configuration files.
    *   For subsequent runs where the image doesn't need rebuilding, you can simply use `docker-compose up`.

3.  **Stopping the services**:
    To stop all running services, press `Ctrl+C` in the terminal where `docker-compose up` is running, and then run:
    ```bash
    docker-compose down
    ```
    To stop the services and remove associated volumes (like Elasticsearch data), use:
    ```bash
    docker-compose down -v
    ```

### Accessing Services

*   **Elasticsearch**: Accessible at `http://localhost:9200` from your host machine.
*   **Kafka**: The Kafka broker is accessible at `localhost:9092` from your host machine (useful for Kafka command-line tools or other clients).
*   **Spark Application Logs**: You typically don't access the Spark application directly. View its logs using:
    ```bash
    docker-compose logs spark-app
    ```
    And to follow the logs:
    ```bash
    docker-compose logs -f spark-app
    ```

### Initial Data for Elasticsearch (Important for Testing)

For the Spark application to read and process data, the configured Elasticsearch index (e.g., `my-index` as per `elasticsearch.properties`) must exist and contain some documents.

After starting the services with `docker-compose up`, you can manually add sample data to Elasticsearch using cURL in a new terminal:

```bash
# Example: Index a sample document into 'my-index'
curl -X POST "localhost:9200/my-index/_doc/1" -H 'Content-Type: application/json' -d'
{
  "user": "kimchy",
  "post_date": "2009-11-15T14:12:12",
  "message": "trying out Elasticsearch in Docker"
}
'

# Verify the data
curl -X GET "localhost:9200/my-index/_search?q=*:*"
```

**Kafka Topic**:
The Kafka topic specified in `kafka.properties` (e.g., `my-topic`) will typically be auto-created by the Kafka broker when the application first tries to publish to it, provided the broker's `auto.create.topics.enable` setting is true (which is common for Confluent Kafka images). If not, the application might log errors if it cannot publish to a non-existent topic.

## Development

*   Import the project into your favorite IDE as a Maven project.
*   Ensure your IDE is configured to use the correct JDK version.
*   You can run the `SparkElasticsearchKafkaApp` main class directly from your IDE for development and testing, provided Spark can run in local mode and the necessary configurations are accessible.
