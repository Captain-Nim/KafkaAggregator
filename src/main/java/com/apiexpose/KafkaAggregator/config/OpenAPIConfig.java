package com.apiexpose.KafkaAggregator.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * OpenAPI (Swagger) configuration following API-First approach
 */
@Configuration
public class OpenAPIConfig {

    @Value("${server.port:8080}")
    private String serverPort;

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Kafka Data Aggregation API")
                        .version("1.0.0")
                        .description("""
                    Real-time data aggregation system for multiple Kafka topics.
                    
                    ## Features
                    - Consumes data from multiple Kafka topics
                    - Aggregates and stores data in PostgreSQL
                    - Real-time updates via Server-Sent Events (SSE)
                    - RESTful API for data retrieval
                    
                    ## How to use the /updates endpoint
                    Open the following URL in your browser:
                    ```
                    http://localhost:8080/updates
                    ```
                    
                    You will see real-time messages like:
                    ```
                    Hi, sensor-data-1 is sending the data from Building-A
                    Hi, sensor-data-2 is sending the data from Building-B
                    ```
                    """)
                        .contact(new Contact()
                                .name("API Support")
                                .email("support@example.com")
                                .url("https://example.com"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0.html")))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:" + serverPort)
                                .description("Local development server")
                ));
    }
}