package io.confluent.developer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class SchemaRegistryConfigTest {
    
    private Properties baseConfig;
    
    @BeforeEach
    void setUp() {
        baseConfig = new Properties();
    }

    @Test
    void testBuildSchemaRegistryConfigMapWithAllProperties() {
        // Arrange
        String expectedUrl = "http://localhost:8081";
        String expectedAuthSource = "USER_INFO";
        String expectedUserInfo = "testuser:testpassword";
        
        baseConfig.put(SCHEMA_REGISTRY_URL_CONFIG, expectedUrl);
        baseConfig.put(BASIC_AUTH_CREDENTIALS_SOURCE, expectedAuthSource);
        baseConfig.put(USER_INFO_CONFIG, expectedUserInfo);

        // Act
        Map<String, ?> result = KafkaStreamsApplication.buildSchemaRegistryConfigMap(baseConfig);

        // Assert
        assertAll(
            () -> assertEquals(3, result.size(), "Should contain exactly three entries"),
            () -> assertEquals(expectedUrl, result.get(SCHEMA_REGISTRY_URL_CONFIG), "Schema Registry URL should match"),
            () -> assertEquals(expectedAuthSource, result.get(BASIC_AUTH_CREDENTIALS_SOURCE), "Auth source should match"),
            () -> assertEquals(expectedUserInfo, result.get(USER_INFO_CONFIG), "User info should match")
        );
    }

    @Test
    void testBuildSchemaRegistryConfigMapWithOnlyUrl() {
        // Arrange
        String expectedUrl = "http://localhost:8081";
        baseConfig.put(SCHEMA_REGISTRY_URL_CONFIG, expectedUrl);

        // Act
        Map<String, ?> result = KafkaStreamsApplication.buildSchemaRegistryConfigMap(baseConfig);

        // Assert
        assertAll(
            () -> assertEquals(1, result.size(), "Should contain exactly one entry"),
            () -> assertEquals(expectedUrl, result.get(SCHEMA_REGISTRY_URL_CONFIG), "Schema Registry URL should match"),
            () -> assertFalse(result.containsKey(BASIC_AUTH_CREDENTIALS_SOURCE), "Should not contain auth source"),
            () -> assertFalse(result.containsKey(USER_INFO_CONFIG), "Should not contain user info")
        );
    }

    @Test
    void testBuildSchemaRegistryConfigMapWithUrlAndAuthSourceOnly() {
        // Arrange
        String expectedUrl = "http://localhost:8081";
        String expectedAuthSource = "USER_INFO";
        
        baseConfig.put(SCHEMA_REGISTRY_URL_CONFIG, expectedUrl);
        baseConfig.put(BASIC_AUTH_CREDENTIALS_SOURCE, expectedAuthSource);

        // Act
        Map<String, ?> result = KafkaStreamsApplication.buildSchemaRegistryConfigMap(baseConfig);

        // Assert
        assertAll(
            () -> assertEquals(2, result.size(), "Should contain exactly two entries"),
            () -> assertEquals(expectedUrl, result.get(SCHEMA_REGISTRY_URL_CONFIG), "Schema Registry URL should match"),
            () -> assertEquals(expectedAuthSource, result.get(BASIC_AUTH_CREDENTIALS_SOURCE), "Auth source should match"),
            () -> assertFalse(result.containsKey(USER_INFO_CONFIG), "Should not contain user info")
        );
    }

    @Test
    void testBuildSchemaRegistryConfigMapWithEmptyProperties() {
        // Act
        Map<String, ?> result = KafkaStreamsApplication.buildSchemaRegistryConfigMap(new Properties());

        // Assert
        assertTrue(result.isEmpty(), "Result map should be empty");
    }

    @Test
    void testBuildSchemaRegistryConfigMapWithInvalidValues() {
        // Arrange
        baseConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "");
        baseConfig.put(BASIC_AUTH_CREDENTIALS_SOURCE, "");
        baseConfig.put(USER_INFO_CONFIG, "");

        // Act
        Map<String, ?> result = KafkaStreamsApplication.buildSchemaRegistryConfigMap(baseConfig);

        // Assert
        assertAll(
            () -> assertEquals(3, result.size(), "Should contain three entries even with empty values"),
            () -> assertEquals("", result.get(SCHEMA_REGISTRY_URL_CONFIG), "Empty URL should be preserved"),
            () -> assertEquals("", result.get(BASIC_AUTH_CREDENTIALS_SOURCE), "Empty auth source should be preserved"),
            () -> assertEquals("", result.get(USER_INFO_CONFIG), "Empty user info should be preserved")
        );
    }
}