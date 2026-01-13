package io.confluent.developer.metrics;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestApiMetrics implements DynamicMBean {
    private static final Logger logger = LoggerFactory.getLogger(RestApiMetrics.class);
    
    // Track metrics per path
    private final ConcurrentHashMap<String, LatencyTracker> pathMetrics = new ConcurrentHashMap<>();
    
    public RestApiMetrics() {
        // Register this MBean
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("io.confluent.developer:type=RestApiMetrics");
            mbs.registerMBean(this, name);
            logger.info("RestApiMetrics MBean registered successfully");
        } catch (Exception e) {
            logger.error("Failed to register MBean", e);
            throw new RuntimeException("Failed to register MBean", e);
        }
    }
    
    /**
     * Record a request for a specific path
     */
    public void recordRequest(String path, long latencyMs) {
        // Store the path as-is (no sanitization needed)
        LatencyTracker tracker = pathMetrics.computeIfAbsent(path, k -> {
            logger.debug("Creating new tracker for path: '{}'", k);
            return new LatencyTracker();
        });
        tracker.recordLatency(latencyMs);
        logger.debug("✅ Recorded - Path: '{}', Latency: {}ms, Count: {}, P90: {}, P999: {}", 
                path, latencyMs, tracker.getRequestCount(), tracker.getP90(), tracker.getP999());
    }
    
    // DynamicMBean implementation
    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException {
        logger.debug("🔍 getAttribute called for: '{}'", attribute);
        
        // Parse attributes like "RequestCount_/inventory/1"
        if (attribute.startsWith("RequestCount_")) {
            String sanitizedPath = attribute.substring("RequestCount_".length());
            logger.debug("   → Looking for RequestCount, sanitizedPath: '{}'", sanitizedPath);
            logger.debug("   → Available keys in pathMetrics: {}", pathMetrics.keySet());
            
            LatencyTracker tracker = pathMetrics.get(sanitizedPath);
            if (tracker != null) {
                long count = tracker.getRequestCount();
                logger.debug("   → Found tracker, returning count: {}", count);
                return count;
            } else {
                logger.warn("   ⚠️ Tracker not found for sanitizedPath: '{}'", sanitizedPath);
                return 0L;
            }
        }
        
        if (attribute.startsWith("LatencyP50_")) {
            String path = attribute.substring("LatencyP50_".length());
            LatencyTracker tracker = pathMetrics.get(path);
            return tracker != null ? round(tracker.getP50(), 5) : 0.0;
        }
        
        if (attribute.startsWith("LatencyP90_")) {
            String path = attribute.substring("LatencyP90_".length());
            LatencyTracker tracker = pathMetrics.get(path);
            return tracker != null ? round(tracker.getP90(), 5) : 0.0;
        }
        
        if (attribute.startsWith("LatencyP95_")) {
            String path = attribute.substring("LatencyP95_".length());
            LatencyTracker tracker = pathMetrics.get(path);
            return tracker != null ? round(tracker.getP95(), 5) : 0.0;
        }
        
        if (attribute.startsWith("LatencyP99_")) {
            String path = attribute.substring("LatencyP99_".length());
            LatencyTracker tracker = pathMetrics.get(path);
            return tracker != null ? round(tracker.getP99(), 5) : 0.0;
        }
        
        if (attribute.startsWith("LatencyP999_")) {
            String path = attribute.substring("LatencyP999_".length());
            LatencyTracker tracker = pathMetrics.get(path);
            return tracker != null ? round(tracker.getP999(), 5) : 0.0;
        }
        
        
        throw new AttributeNotFoundException("Attribute " + attribute + " not found");
    }

    /**
     * Round a double to specified decimal places
     */
    private double round(double value, int decimalPlaces) {
        double scale = Math.pow(10, decimalPlaces);
        return Math.round(value * scale) / scale;
    }

    @Override
    public void setAttribute(Attribute attribute) throws AttributeNotFoundException {
        throw new AttributeNotFoundException("No attributes can be set");
    }
    
    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();
        for (String attr : attributes) {
            try {
                list.add(new Attribute(attr, getAttribute(attr)));
            } catch (AttributeNotFoundException e) {
                logger.warn("Attribute not found: {}", attr);
            }
        }
        return list;
    }
    
    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }
    
    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) {
        return null;
    }
    
    @Override
    public MBeanInfo getMBeanInfo() {
        logger.debug("getMBeanInfo called, paths tracked: {}", pathMetrics.keySet());
        
        // Build attribute list dynamically
        java.util.List<MBeanAttributeInfo> attributesList = new java.util.ArrayList<>();
        
        // Add per-path attributes
        for (String path : pathMetrics.keySet()) {
            attributesList.add(new MBeanAttributeInfo(
                "RequestCount_" + path,
                "long",
                "Request count for path: " + path,
                true, false, false
            ));
            
            attributesList.add(new MBeanAttributeInfo(
                "LatencyP50_" + path,
                "double",
                "P50 (median) latency for path: " + path,
                true, false, false
            ));
            
            attributesList.add(new MBeanAttributeInfo(
                "LatencyP90_" + path,
                "double",
                "P90 latency for path: " + path,
                true, false, false
            ));
            
            attributesList.add(new MBeanAttributeInfo(
                "LatencyP95_" + path,
                "double",
                "P95 latency for path: " + path,
                true, false, false
            ));
            
            attributesList.add(new MBeanAttributeInfo(
                "LatencyP99_" + path,
                "double",
                "P99 latency for path: " + path,
                true, false, false
            ));
            
            attributesList.add(new MBeanAttributeInfo(
                "LatencyP999_" + path,
                "double",
                "P99.9 latency for path: " + path,
                true, false, false
            ));
        }
        
        MBeanAttributeInfo[] attributes = attributesList.toArray(new MBeanAttributeInfo[0]);
        
        return new MBeanInfo(
            this.getClass().getName(),
            "REST API Metrics",
            attributes,
            null, // constructors
            null, // operations
            null  // notifications
        );
    }
    
    /**
     * Sanitize path for use in attribute names
     * Replace / with _ and remove other special characters
     */
    private String sanitizePath(String path) {
        return path.replace("/", "_").replace("{", "").replace("}", "");
    }
    
    /**
     * Get all tracked paths (useful for debugging)
     */
    public java.util.Set<String> getTrackedPaths() {
        return pathMetrics.keySet();
    }
}