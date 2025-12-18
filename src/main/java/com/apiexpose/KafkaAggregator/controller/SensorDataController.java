package com.apiexpose.KafkaAggregator.controller;

import com.apiexpose.KafkaAggregator.dto.SensorDataDTO;
import com.apiexpose.KafkaAggregator.service.SensorDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST Controller for SensorData CRUD operations
 */
@RestController
@RequestMapping("/api/sensor-data")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Sensor Data", description = "CRUD operations for sensor data")
public class SensorDataController {

    private final SensorDataService sensorDataService;

    /**
     * Get all sensor data with pagination
     */
    @GetMapping
    @Operation(
            summary = "Get all sensor data",
            description = "Retrieve all aggregated sensor data from the database with pagination"
    )
    public ResponseEntity<Page<SensorDataDTO>> getAllSensorData(
            @Parameter(description = "Page number (0-indexed)")
            @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size")
            @RequestParam(defaultValue = "20") int size,
            @Parameter(description = "Sort field")
            @RequestParam(defaultValue = "timestamp") String sortBy,
            @Parameter(description = "Sort direction (asc/desc)")
            @RequestParam(defaultValue = "desc") String direction) {

        Sort.Direction sortDirection = direction.equalsIgnoreCase("asc")
                ? Sort.Direction.ASC
                : Sort.Direction.DESC;

        Pageable pageable = PageRequest.of(page, size, Sort.by(sortDirection, sortBy));
        Page<SensorDataDTO> sensorData = sensorDataService.getAllSensorData(pageable);

        return ResponseEntity.ok(sensorData);
    }

    /**
     * Get sensor data by ID
     */
    @GetMapping("/{id}")
    @Operation(
            summary = "Get sensor data by ID",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Sensor data found"),
                    @ApiResponse(responseCode = "404", description = "Sensor data not found")
            }
    )
    public ResponseEntity<SensorDataDTO> getSensorDataById(
            @Parameter(description = "Sensor data ID")
            @PathVariable Long id) {

        try {
            SensorDataDTO sensorData = sensorDataService.getSensorDataById(id);
            return ResponseEntity.ok(sensorData);
        } catch (RuntimeException e) {
            log.error("SensorData not found with id: {}", id);
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Get sensor data by topic name
     */
    @GetMapping("/topic/{topicName}")
    @Operation(
            summary = "Get sensor data by topic name",
            description = "Retrieve all sensor data for a specific Kafka topic"
    )
    public ResponseEntity<List<SensorDataDTO>> getSensorDataByTopic(
            @Parameter(description = "Kafka topic name")
            @PathVariable String topicName) {

        List<SensorDataDTO> sensorData = sensorDataService.getSensorDataByTopic(topicName);
        return ResponseEntity.ok(sensorData);
    }

    /**
     * Get sensor data by location
     */
    @GetMapping("/location/{location}")
    @Operation(
            summary = "Get sensor data by location",
            description = "Retrieve all sensor data for a specific location"
    )
    public ResponseEntity<List<SensorDataDTO>> getSensorDataByLocation(
            @Parameter(description = "Location name")
            @PathVariable String location) {

        List<SensorDataDTO> sensorData = sensorDataService.getSensorDataByLocation(location);
        return ResponseEntity.ok(sensorData);
    }

    /**
     * Get latest sensor data for each topic
     */
    @GetMapping("/latest")
    @Operation(
            summary = "Get latest sensor data for each topic",
            description = "Retrieve the most recent sensor data entry for each Kafka topic"
    )
    public ResponseEntity<List<SensorDataDTO>> getLatestDataForEachTopic() {
        List<SensorDataDTO> latestData = sensorDataService.getLatestDataForEachTopic();
        return ResponseEntity.ok(latestData);
    }
}