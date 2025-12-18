package com.apiexpose.KafkaAggregator.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Test producer to simulate devices sending data to Kafka topics
 * This runs automatically when the application starts
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaTestProducer implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();

    private final String[] topics = {"sensor-data-1", "sensor-data-2", "sensor-data-3"};
    private final String[] locations = {"Building-A", "Building-B", "Building-C"};

    @Override
    public void run(String... args) {
        // Uncomment the following line to enable automatic test data generation
         startSimulation();

        log.info("Kafka Test Producer initialized. Call startSimulation() to begin sending test data.");
    }

    /**
     * Start simulating device data
     * Sends messages to Kafka topics every 5 seconds
     */
    public void startSimulation() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        executor.scheduleAtFixedRate(() -> {
            try {
                for (int i = 0; i < topics.length; i++) {
                    sendTestMessage(topics[i], locations[i]);
                }
            } catch (Exception e) {
                log.error("Error in simulation", e);
            }
        }, 0, 5, TimeUnit.SECONDS);

        log.info("Kafka simulation started. Sending messages every 5 seconds to {} topics", topics.length);
    }

    /**
     * Send a test message to a specific topic
     */
    public void sendTestMessage(String topic, String location) {
        try {
            Map<String, Object> data = new HashMap<>();
            data.put("location", location);
            data.put("deviceId", "device-" + random.nextInt(100));
            data.put("temperature", 20 + random.nextDouble() * 15);
            data.put("humidity", 40 + random.nextDouble() * 40);
            data.put("timestamp", System.currentTimeMillis());
            data.put("status", "active");

            String jsonMessage = objectMapper.writeValueAsString(data);

            kafkaTemplate.send(topic, jsonMessage);

            log.info("Sent message to topic '{}': {}", topic, jsonMessage);

        } catch (Exception e) {
            log.error("Error sending test message to topic '{}'", topic, e);
        }
    }
}