package com.apiexpose.KafkaAggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main application class for Kafka Data Aggregation Service
 */

@SpringBootApplication
public class KafkaAggregatorApplication {

	public static void main(String[] args) {
        SpringApplication.run(KafkaAggregatorApplication.class, args);

        System.out.println("\n" + "=".repeat(80));
        System.out.println("Kafka Data Aggregation Service Started Successfully!");
        System.out.println("=".repeat(80));
        System.out.println("OpenAPI Specification: http://localhost:8080/api-docs");
        System.out.println("Swagger UI: http://localhost:8080/swagger-ui.html");
        System.out.println("Real-time Updates (SSE): http://localhost:8080/updates");
        System.out.println("=".repeat(80) + "\n");
    }

}