package com.apiexpose.KafkaAggregator.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

//DTO for SensorData Response

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SensorDataDTO
{
    private Long id;
    private String topicName;
    private String location;
    private String data;
    private LocalDateTime timestamp;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
