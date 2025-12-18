package com.apiexpose.KafkaAggregator.consumer;

import com.apiexpose.KafkaAggregator.service.SensorDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Kafka consumer that listens to multiple topics and processes messages
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaMessageConsumer {

    private final SensorDataService sensorDataService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Listen to multiple Kafka topics
     * Topics are configured in application.yml under spring.kafka.topics
     */
    @KafkaListener(
            topics = {"sensor-data-1", "sensor-data-2", "sensor-data-3"},
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.info("Received message from topic '{}': {}", topic, message);

        try {
            // Parse the message to extract location
            String location = extractLocation(message, topic);

            // Save to database and trigger SSE notification
            sensorDataService.saveSensorData(topic, location, message);

            log.info("Successfully processed message from topic '{}'", topic);

        } catch (Exception e) {
            log.error("Error processing message from topic '{}': {}", topic, e.getMessage(), e);
        }
    }

    /**
     * Extract location from the message payload
     * Assumes message is JSON with a "location" field
     * Falls back to topic-based location mapping if not present
     */
    private String extractLocation(String message, String topic) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);

            // Try to get location from message
            if (jsonNode.has("location")) {
                return jsonNode.get("location").asText();
            }

            // Try to get deviceId and use it as location identifier
            if (jsonNode.has("deviceId")) {
                return "Device-" + jsonNode.get("deviceId").asText();
            }

        } catch (Exception e) {
            log.debug("Could not parse message as JSON, using default location mapping", e);
        }

        // Default location mapping based on topic
        return getDefaultLocationForTopic(topic);
    }

    /**
     * Map topics to default locations
     */
    private String getDefaultLocationForTopic(String topic) {
        return switch (topic) {
            case "sensor-data-1" -> "Building-A";
            case "sensor-data-2" -> "Building-B";
            case "sensor-data-3" -> "Building-C";
            default -> "Unknown-Location";
        };
    }
}