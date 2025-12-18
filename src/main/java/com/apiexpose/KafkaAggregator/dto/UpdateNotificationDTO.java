package com.apiexpose.KafkaAggregator.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// DTO for SSE Update Message

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateNotificationDTO
{
    private String message;
    private String location;
    private String topicName;
    private LocalDateTime timestamp;

    public String toSSEFormat()
    {
        return String.format("Hi, %s is sending the data from %s", topicName, location);
    }

}
