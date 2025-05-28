# Use an official Spark image as a base
# Ensure this Spark version is compatible with the one used in pom.xml (e.g., 3.4.1)
# You might need to specify a version with a specific Scala version too (e.g., 3.4.1-scala2.12)
FROM bitnami/spark:3.4.1

USER root

# Spark application JAR name (update if your artifactId or version changes)
ARG APP_JAR="spark-elasticsearch-kafka-1.0-SNAPSHOT.jar"
ARG APP_NAME="spark-elasticsearch-kafka"

ENV SPARK_APPLICATION_JAR_NAME=${APP_JAR}
ENV SPARK_APPLICATION_MAIN_CLASS=com.example.SparkElasticsearchKafkaApp
# ENV SPARK_MASTER_URL=spark://spark-master:7077 # This will be overridden by Docker Compose for local mode

WORKDIR /opt/bitnami/spark/app

# Copy the fat JAR
# The JAR needs to be built first, e.g., using 'mvn clean package'
# This Dockerfile assumes the JAR is in the 'target' directory relative to the Dockerfile context
COPY target/${APP_JAR} ./

# Copy configuration files
COPY src/main/resources/elasticsearch.properties ./config/elasticsearch.properties
COPY src/main/resources/kafka.properties ./config/kafka.properties
COPY src/main/resources/log4j2.properties ./config/log4j2.properties

# The spark-submit command will be typically overridden or fully specified in docker-compose.yml
# However, we can set a default entrypoint or command if needed.
# For local execution within Docker Compose, the spark-submit will look something like:
# CMD [ "/opt/bitnami/spark/bin/spark-submit", "--class", "com.example.SparkElasticsearchKafkaApp", #       "--master", "local[*]", "--deploy-mode", "client", #       "--name", "ElasticsearchToKafka", #       "--conf", "spark.driver.extraClassPath=./config", \ # To pick up log4j2.properties
#       "--conf", "spark.executor.extraClassPath=./config", \ # To pick up log4j2.properties
#       "./${SPARK_APPLICATION_JAR_NAME}" ]
# The actual command in docker-compose will handle dependencies (ES and Kafka jars) via --packages

# Ensure the config directory is writable if needed by any process, though typically read-only is fine for these props.
RUN mkdir -p /opt/bitnami/spark/conf &&     chmod -R g+w /opt/bitnami/spark/conf &&     chmod -R g+w /opt/bitnami/spark/app/config

USER 1001 # Default bitnami user

# The CMD can be set here, but often it's more flexible to set it in docker-compose.yml
# For example:
# CMD ["sh", "-c", "echo 'Application JAR and configs are ready. Use docker-compose to run.'"]
