package io.confluent.developer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class for constructing Avro GenericRecord keys from simple values
 */
public class AvroKeyBuilder {
    
    private static final Logger logger = LoggerFactory.getLogger(AvroKeyBuilder.class);
    
    // Cache schemas by subject to avoid repeated lookups during runtime
    private static final Map<String, ParsedSchema> SCHEMA_CACHE = new ConcurrentHashMap<>();
    
    /**
     * Builds an Avro GenericRecord key based on the topic's key schema
     * 
     * @param schemaRegistryClient Schema Registry client
     * @param topic Kafka topic name
     * @param keyFieldName Name of the field in the key schema to populate
     * @param keyFieldValue Value to set for the key field
     * @return GenericRecord representing the key
     */
    public static GenericRecord buildKey(
            SchemaRegistryClient schemaRegistryClient,
            String topic,
            String keyFieldName,
            String keyFieldValue) throws IOException, RestClientException {
        
        String subject = topic + "-key";
        
        // Check cache first
        ParsedSchema parsedSchema = SCHEMA_CACHE.get(subject);
        if (parsedSchema == null) {
            // Cache miss - fetch from schema registry (only happens once per subject)
            logger.info("Schema cache miss for subject '{}', fetching from registry", subject);
            int schemaId = schemaRegistryClient.getLatestSchemaMetadata(subject).getId();
            parsedSchema = schemaRegistryClient.getSchemaById(schemaId);
            SCHEMA_CACHE.put(subject, parsedSchema);
            logger.info("Cached schema for subject '{}'", subject);
        }
        
        // Cast to AvroSchema to get the raw Avro Schema
        if (!(parsedSchema instanceof AvroSchema)) {
            throw new IllegalArgumentException(
                "Expected Avro schema but got: " + parsedSchema.schemaType());
        }
        
        AvroSchema avroSchema = (AvroSchema) parsedSchema;
        Schema keySchema = avroSchema.rawSchema();
        
        logger.debug("Retrieved key schema for topic '{}': {}", topic, keySchema);
        
        // Create a new GenericRecord
        GenericRecord key = new GenericData.Record(keySchema);
        
        // Find the field in the schema
        Schema.Field field = keySchema.getField(keyFieldName);
        if (field == null) {
            throw new IllegalArgumentException(
                String.format("Field '%s' not found in key schema for topic '%s'. Available fields: %s",
                    keyFieldName, topic, keySchema.getFields()));
        }
        
        // Set the field value with proper type conversion
        Object typedValue = convertToSchemaType(keyFieldValue, field.schema());
        key.put(keyFieldName, typedValue);
        
        // Set default values for other fields if they exist
        for (Schema.Field schemaField : keySchema.getFields()) {
            if (!schemaField.name().equals(keyFieldName)) {
                if (schemaField.defaultVal() != null) {
                    key.put(schemaField.name(), schemaField.defaultVal());
                }
            }
        }
        
        logger.debug("Built Avro key: {}", key);
        return key;
    }
    
    /**
     * Converts a string value to the appropriate type based on Avro schema
     */
    private static Object convertToSchemaType(String value, Schema schema) {
        // Handle union types (e.g., ["null", "string"])
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema unionSchema : schema.getTypes()) {
                if (unionSchema.getType() != Schema.Type.NULL) {
                    return convertToSchemaType(value, unionSchema);
                }
            }
        }
        
        // Convert based on the actual type
        switch (schema.getType()) {
            case STRING:
                return value;
            case INT:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            default:
                // For complex types, return as string and let Avro handle it
                return value;
        }
    }
    
    /**
     * Builds an Avro key with multiple fields
     * 
     * @param schemaRegistryClient Schema Registry client
     * @param topic Kafka topic name
     * @param keyFields Map of field names to values
     * @return GenericRecord representing the key
     */
    public static GenericRecord buildKey(
            SchemaRegistryClient schemaRegistryClient,
            String topic,
            Map<String, String> keyFields) throws IOException, RestClientException {
        
        String subject = topic + "-key";
        
        // Check cache first
        ParsedSchema parsedSchema = SCHEMA_CACHE.get(subject);
        if (parsedSchema == null) {
            // Cache miss - fetch from schema registry (only happens once per subject)
            logger.info("Schema cache miss for subject '{}', fetching from registry", subject);
            int schemaId = schemaRegistryClient.getLatestSchemaMetadata(subject).getId();
            parsedSchema = schemaRegistryClient.getSchemaById(schemaId);
            SCHEMA_CACHE.put(subject, parsedSchema);
            logger.info("Cached schema for subject '{}'", subject);
        }
        
        if (!(parsedSchema instanceof AvroSchema)) {
            throw new IllegalArgumentException(
                "Expected Avro schema but got: " + parsedSchema.schemaType());
        }
        
        AvroSchema avroSchema = (AvroSchema) parsedSchema;
        Schema keySchema = avroSchema.rawSchema();
        
        GenericRecord key = new GenericData.Record(keySchema);
        
        // Set all provided fields
        for (Map.Entry<String, String> entry : keyFields.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            
            Schema.Field field = keySchema.getField(fieldName);
            if (field == null) {
                throw new IllegalArgumentException(
                    String.format("Field '%s' not found in key schema", fieldName));
            }
            
            Object typedValue = convertToSchemaType(fieldValue, field.schema());
            key.put(fieldName, typedValue);
        }
        
        // Set defaults for remaining fields
        for (Schema.Field field : keySchema.getFields()) {
            if (!keyFields.containsKey(field.name()) && field.defaultVal() != null) {
                key.put(field.name(), field.defaultVal());
            }
        }
        
        return key;
    }
    
    /**
     * Alternative approach: Parse schema string directly
     * This avoids the ParsedSchema API entirely
     */
    public static GenericRecord buildKeyFromString(
            SchemaRegistryClient schemaRegistryClient,
            String topic,
            String keyFieldName,
            String keyFieldValue) throws IOException, RestClientException {
        
        String subject = topic + "-key";
        
        // Get schema as string
        String schemaString = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
        
        // Parse it into Schema object
        Schema keySchema = new Schema.Parser().parse(schemaString);
        
        logger.debug("Parsed key schema for topic '{}': {}", topic, keySchema);
        
        // Create GenericRecord
        GenericRecord key = new GenericData.Record(keySchema);
        
        Schema.Field field = keySchema.getField(keyFieldName);
        if (field == null) {
            throw new IllegalArgumentException(
                String.format("Field '%s' not found in key schema", keyFieldName));
        }
        
        Object typedValue = convertToSchemaType(keyFieldValue, field.schema());
        key.put(keyFieldName, typedValue);
        
        // Set defaults
        for (Schema.Field schemaField : keySchema.getFields()) {
            if (!schemaField.name().equals(keyFieldName) && schemaField.defaultVal() != null) {
                key.put(schemaField.name(), schemaField.defaultVal());
            }
        }
        
        return key;
    }
}