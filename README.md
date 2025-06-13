# Spark Kafka Consumers Example

This project demonstrates two Spark Structured Streaming consumers reading from the same Kafka topic, each belonging to a different consumer group, thus allowing both to process all messages independently. A simple Kafka producer is also included for testing.

## Prerequisites

*   Java Development Kit (JDK 8 or 11 recommended)
*   sbt (Scala Build Tool) - version 1.x
*   A running Kafka instance.
    *   Broker typically at `localhost:9092`.
    *   A topic named `test-topic` should be created. You can create it using Kafka's command-line tools:
        ```bash
        # Example:
        # KAFKA_HOME/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
        ```

## Building the Project

To build the project and create an assembly JAR (fat JAR):
```bash
sbt clean compile assembly
```
The assembly JAR will be located in `target/scala-2.12/spark-kafka-consumers-assembly-0.1.0-SNAPSHOT.jar`. (Note: The exact path might vary slightly based on sbt version or project settings).

## Running the Applications

You will need multiple terminal windows to run the producer and consumers simultaneously.

### 1. Start Kafka
Ensure your Kafka server (and Zookeeper) is running.

### 2. Run the Kafka Producer (Optional)
This application will send 10 sample messages to `test-topic`.
```bash
sbt "runMain com.example.KafkaProducerApp"
```
Alternatively, using the assembly JAR:
```bash
java -cp target/scala-2.12/spark-kafka-consumers-assembly-0.1.0-SNAPSHOT.jar com.example.KafkaProducerApp
```

### 3. Run Spark Consumer 1
```bash
sbt "runMain com.example.SparkConsumer1App"
```
Alternatively, using `spark-submit` with the assembly JAR (ensure `$SPARK_HOME` is set or use the full path to `spark-submit`):
```bash
# $SPARK_HOME/bin/spark-submit --class com.example.SparkConsumer1App target/scala-2.12/spark-kafka-consumers-assembly-0.1.0-SNAPSHOT.jar
```

### 4. Run Spark Consumer 2
In a new terminal:
```bash
sbt "runMain com.example.SparkConsumer2App"
```
Alternatively, using `spark-submit` with the assembly JAR (ensure `$SPARK_HOME` is set or use the full path to `spark-submit`):
```bash
# $SPARK_HOME/bin/spark-submit --class com.example.SparkConsumer2App target/scala-2.12/spark-kafka-consumers-assembly-0.1.0-SNAPSHOT.jar
```

## Expected Behavior

After starting the producer (if you choose to run it), both `SparkConsumer1App` and `SparkConsumer2App` will start listening for messages on `test-topic`. When messages are produced to the topic, they will be printed to the respective consoles of each consumer. Each consumer will process all messages independently due to their different consumer group IDs. If you send the 10 sample messages from `KafkaProducerApp`, both consumers should display them.

## Testing

To run the placeholder test suite:
```bash
sbt test
```
