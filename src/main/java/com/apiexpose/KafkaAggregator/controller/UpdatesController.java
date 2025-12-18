package com.apiexpose.KafkaAggregator.controller;

import com.apiexpose.KafkaAggregator.service.SSEService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Controller for Server-Sent Events (SSE) real-time updates
 */
@RestController
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Real-Time Updates", description = "Server-Sent Events for real-time notifications")
public class UpdatesController {

    private final SSEService sseService;

    /**
     * SSE endpoint for real-time updates
     * Opens in browser: http://localhost:8080/updates
     */
    @GetMapping(value = "/updates", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(
            summary = "Subscribe to real-time topic updates",
            description = "Server-Sent Events (SSE) endpoint that streams real-time notifications " +
                    "whenever data is received from Kafka topics. " +
                    "Each event message format: 'Hi, <TopicName> is sending the data from <Location>'",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "SSE stream established",
                            content = @Content(
                                    mediaType = "text/event-stream",
                                    examples = @ExampleObject(
                                            value = "event: update\ndata: Hi, sensor-data-1 is sending the data from Building-A\n\n"
                                    )
                            )
                    )
            }
    )
    public SseEmitter streamUpdates() {
        log.info("New SSE connection requested from /updates endpoint");
        return sseService.createEmitter();
    }
}
