package com.apiexpose.KafkaAggregator.service;

import com.apiexpose.KafkaAggregator.dto.SensorDataDTO;
import com.apiexpose.KafkaAggregator.entity.SensorData;
import com.apiexpose.KafkaAggregator.repository.SensorDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service layer for SensorData operations
 */

@Service
@RequiredArgsConstructor
@Slf4j
public class SensorDataService
{
    private final SensorDataRepository repository;
    private final SSEService sseService;

    /**
     * Save sensor data to database and notify SSE clients
     */
    @Transactional
    public SensorDataDTO saveSensorData(String topicName, String location, String data) {
        log.info("Saving sensor data - Topic: {}, Location: {}", topicName, location);

        SensorData sensorData = SensorData.builder()
                .topicName(topicName)
                .location(location)
                .data(data)
                .timestamp(LocalDateTime.now())
                .build();

        SensorData savedData = repository.save(sensorData);

        // Broadcast update to SSE clients
        sseService.broadcastUpdate(topicName, location);

        log.info("Sensor data saved successfully with ID: {}", savedData.getId());

        return convertToDTO(savedData);
    }

    /**
     * Get all sensor data with pagination
     */
    public Page<SensorDataDTO> getAllSensorData(Pageable pageable) {
        return repository.findAll(pageable).map(this::convertToDTO);
    }

    /**
     * Get sensor data by ID
     */
    public SensorDataDTO getSensorDataById(Long id) {
        return repository.findById(id)
                .map(this::convertToDTO)
                .orElseThrow(() -> new RuntimeException("SensorData not found with id: " + id));
    }

    /**
     * Get sensor data by topic name
     */
    public List<SensorDataDTO> getSensorDataByTopic(String topicName) {
        return repository.findByTopicName(topicName)
                .stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }

    /**
     * Get sensor data by location
     */
    public List<SensorDataDTO> getSensorDataByLocation(String location) {
        return repository.findByLocation(location)
                .stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }

    /**
     * Get latest sensor data for each topic
     */
    public List<SensorDataDTO> getLatestDataForEachTopic() {
        return repository.findLatestByEachTopic()
                .stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }

    /**
     * Convert entity to DTO
     */
    private SensorDataDTO convertToDTO(SensorData entity) {
        return SensorDataDTO.builder()
                .id(entity.getId())
                .topicName(entity.getTopicName())
                .location(entity.getLocation())
                .data(entity.getData())
                .timestamp(entity.getTimestamp())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .build();
    }
}
