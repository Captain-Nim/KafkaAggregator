package com.apiexpose.KafkaAggregator.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// DTO for Kafka Message

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaMessageDTO
{
    private String location;
    private String deviceId;
    private String data;
    private Long timestamp;

}
