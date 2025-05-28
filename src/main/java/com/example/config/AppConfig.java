package com.example.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private static final Logger logger = LogManager.getLogger(AppConfig.class);
    private Properties properties;

    public AppConfig(String fileName) {
        properties = new Properties();
        try (InputStream inputStream = AppConfig.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                logger.error("Configuration file not found: {}", fileName);
                throw new RuntimeException("Configuration file not found: " + fileName);
            }
            properties.load(inputStream);
            logger.info("Successfully loaded configuration file: {}", fileName);
        } catch (IOException e) {
            logger.error("Error loading configuration file: {}", fileName, e);
            throw new RuntimeException("Error loading configuration file: " + fileName, e);
        }
    }

    public StringgetProperty(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            logger.warn("Property not found for key: {}", key);
            // Depending on the property, you might throw an exception or return a default
        }
        // If property not found in file and no default was provided to this specific call of getProperty(key)
        if (value == null) {
             logger.warn("Property '{}' not found in properties file.", key);
             // Depending on requirements, could throw an exception here if the property is mandatory
        }
        return value;
    }

    public String getProperty(String key, String defaultValue) {
        // Convert property key to a potential environment variable name
        // e.g., "es.nodes" -> "ES_NODES", "kafka.bootstrap.servers" -> "KAFKA_BOOTSTRAP_SERVERS"
        String envVarName = key.toUpperCase().replace('.', '_');
        String value = System.getenv(envVarName);

        if (value != null && !value.isEmpty()) {
            logger.info("Using environment variable {} for property '{}'. Value: {}", envVarName, key, value);
            return value;
        }

        // Fallback to properties file
        value = properties.getProperty(key);
        if (value != null) {
            logger.debug("Using property '{}' from file. Value: {}", key, value);
            return value;
        }

        // Fallback to default value
        logger.warn("Property '{}' not found in properties file or environment. Using default value: {}", key, defaultValue);
        return defaultValue;
    }

    public int getIntProperty(String key) {
        // Convert property key to an environment variable name
        String envVarName = key.toUpperCase().replace('.', '_');
        String envValue = System.getenv(envVarName);

        if (envValue != null && !envValue.isEmpty()) {
            try {
                int intVal = Integer.parseInt(envValue);
                logger.info("Using environment variable {} for integer property '{}'. Value: {}", envVarName, key, intVal);
                return intVal;
            } catch (NumberFormatException e) {
                logger.error("Error parsing integer from environment variable {} for property key: {}. Value: {}. Falling back.", envVarName, key, envValue, e);
                // Fallback to properties file or throw error if conversion fails
            }
        }

        String propValue = properties.getProperty(key);
        if (propValue == null) {
            logger.error("Mandatory integer property '{}' not found in properties file or environment.", key);
            throw new RuntimeException("Mandatory integer property not found: " + key);
        }
        try {
            int intVal = Integer.parseInt(propValue);
            logger.debug("Using integer property '{}' from file. Value: {}", key, intVal);
            return intVal;
        } catch (NumberFormatException e) {
            logger.error("Error parsing integer from properties file for property key: {}. Value: {}", key, propValue, e);
            throw new RuntimeException("Invalid integer value for property: " + key + " in properties file", e);
        }
    }

    public int getIntProperty(String key, int defaultValue) {
        // Convert property key to an environment variable name
        String envVarName = key.toUpperCase().replace('.', '_');
        String envValue = System.getenv(envVarName);

        if (envValue != null && !envValue.isEmpty()) {
            try {
                int intVal = Integer.parseInt(envValue);
                logger.info("Using environment variable {} for integer property '{}'. Value: {}", envVarName, key, intVal);
                return intVal;
            } catch (NumberFormatException e) {
                logger.warn("Error parsing integer from environment variable {} for property key: {}. Value: {}. Falling back to properties or default.", envVarName, key, envValue, e);
                // Fallback to properties file or default value
            }
        }

        String propValue = properties.getProperty(key);
        if (propValue != null) {
            try {
                int intVal = Integer.parseInt(propValue);
                logger.debug("Using integer property '{}' from file. Value: {}", key, intVal);
                return intVal;
            } catch (NumberFormatException e) {
                logger.warn("Error parsing integer from properties file for property key: {}. Value: {}. Using default value: {}", key, propValue, defaultValue, e);
                return defaultValue;
            }
        }

        logger.warn("Integer property '{}' not found in properties file or environment. Using default value: {}", key, defaultValue);
        return defaultValue;
    }

    // Example specific getters (can be added as needed)
    // For elasticsearch.properties
    public String getEsNodes() {
        return getProperty("es.nodes");
    }

    public int getEsPort() {
        return getIntProperty("es.port");
    }

    public String getEsScheme() {
        return getProperty("es.scheme");
    }

    public String getEsIndex() {
        return getProperty("es.index");
    }

    public String getEsQuery() {
        return getProperty("es.query");
    }

    // For kafka.properties
    public String getKafkaBootstrapServers() {
        return getProperty("kafka.bootstrap.servers");
    }

    public String getKafkaTopic() {
        return getProperty("kafka.topic");
    }
}
