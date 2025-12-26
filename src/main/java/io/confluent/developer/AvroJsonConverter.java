package io.confluent.developer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.IOException;
import java.util.Collection;

/**
 * Utility class for converting Avro GenericRecord to Jackson JsonNode
 */
public class AvroJsonConverter {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Converts an Avro GenericRecord to a clean JsonNode without type wrappers.
     * This produces a simplified JSON structure that's easier to consume.
     * 
     * @param record The Avro GenericRecord to convert
     * @return A JsonNode with clean field values
     */
    public static JsonNode toJsonNode(GenericRecord record) {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        
        record.getSchema().getFields().forEach(field -> {
            String fieldName = field.name();
            Object fieldValue = record.get(fieldName);
            
            if (fieldValue == null) {
                jsonNode.putNull(fieldName);
            } else if (fieldValue instanceof CharSequence) {
                jsonNode.put(fieldName, fieldValue.toString());
            } else if (fieldValue instanceof Integer) {
                jsonNode.put(fieldName, (Integer) fieldValue);
            } else if (fieldValue instanceof Long) {
                jsonNode.put(fieldName, (Long) fieldValue);
            } else if (fieldValue instanceof Float) {
                jsonNode.put(fieldName, (Float) fieldValue);
            } else if (fieldValue instanceof Double) {
                jsonNode.put(fieldName, (Double) fieldValue);
            } else if (fieldValue instanceof Boolean) {
                jsonNode.put(fieldName, (Boolean) fieldValue);
            } else if (fieldValue instanceof GenericRecord) {
                // Recursively handle nested records
                jsonNode.set(fieldName, toJsonNode((GenericRecord) fieldValue));
            } else if (fieldValue instanceof Collection) {
                // Handle arrays
                ArrayNode arrayNode = objectMapper.createArrayNode();
                ((Collection<?>) fieldValue).forEach(item -> {
                    if (item instanceof GenericRecord) {
                        arrayNode.add(toJsonNode((GenericRecord) item));
                    } else {
                        arrayNode.add(objectMapper.valueToTree(item));
                    }
                });
                jsonNode.set(fieldName, arrayNode);
            } else {
                // Fallback for other types
                jsonNode.set(fieldName, objectMapper.valueToTree(fieldValue));
            }
        });
        
        return jsonNode;
    }
    
    /**
     * Converts an Avro GenericRecord to a JsonNode with type information preserved.
     * This uses Avro's native JSON encoder which includes union type wrappers.
     * 
     * @param record The Avro GenericRecord to convert
     * @return A JsonNode with Avro type information
     * @throws IOException If encoding fails
     */
    public static JsonNode toJsonNodeWithTypes(GenericRecord record) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        writer.write(record, jsonEncoder);
        jsonEncoder.flush();
        return objectMapper.readTree(baos.toString());
    }
}