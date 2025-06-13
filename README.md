# Spark Kafka Consumers Example (Java Version)

This project demonstrates two Spark Structured Streaming consumers, written in Java, reading from the same Kafka topic. Each consumer belongs to a different consumer group, allowing both to process all messages independently. A simple Kafka producer, also in Java, is included for testing.

## Prerequisites

*   Java Development Kit (JDK 8 or 11 recommended)
*   sbt (Scala Build Tool) - version 1.x (Used for managing dependencies and building the project)
*   A running Kafka instance:
    *   Broker typically at `localhost:9092`.
    *   A topic named `test-topic` should be created. You can create it using Kafka's command-line tools:
        ```bash
        # Example (replace KAFKA_HOME with your Kafka installation path):
        # KAFKA_HOME/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
        ```

## Building the Project

To build the project and create an assembly JAR (fat JAR):
```bash
sbt clean compile assembly
```
The assembly JAR will be located in a path similar to `target/scala-2.12/spark-kafka-consumers-java-assembly-0.1.0-SNAPSHOT.jar`. (The `scala-2.12` part of the path remains due to Spark's Scala core, even for a Java project).

## Running the Applications

You will need multiple terminal windows to run the producer and consumers simultaneously.

### 1. Start Kafka
Ensure your Kafka server (and Zookeeper, if applicable) is running.

### 2. Run the Kafka Producer (Optional)
This application will send 10 sample messages to `test-topic`.
```bash
sbt "runMain com.example.KafkaProducerApp"
```
Alternatively, using the assembly JAR:
```bash
java -cp target/scala-2.12/spark-kafka-consumers-java-assembly-0.1.0-SNAPSHOT.jar com.example.KafkaProducerApp
```

### 3. Run Spark Consumer 1
```bash
sbt "runMain com.example.SparkConsumer1App"
```
Alternatively, using `spark-submit` with the assembly JAR:
```bash
# Replace $SPARK_HOME with your Spark installation path
# $SPARK_HOME/bin/spark-submit --class com.example.SparkConsumer1App target/scala-2.12/spark-kafka-consumers-java-assembly-0.1.0-SNAPSHOT.jar
```

### 4. Run Spark Consumer 2
In a new terminal:
```bash
sbt "runMain com.example.SparkConsumer2App"
```
Alternatively, using `spark-submit` with the assembly JAR:
```bash
# Replace $SPARK_HOME with your Spark installation path
# $SPARK_HOME/bin/spark-submit --class com.example.SparkConsumer2App target/scala-2.12/spark-kafka-consumers-java-assembly-0.1.0-SNAPSHOT.jar
```

## Expected Behavior

After starting the producer, both `SparkConsumer1App` and `SparkConsumer2App` will start receiving and printing the messages sent to `test-topic` to their respective consoles. Each consumer will process all 10 messages independently due to their different consumer group IDs.

## Testing

To run the (currently basic) unit tests:
```bash
sbt test
```
This will execute tests defined in `src/test/java/`.
