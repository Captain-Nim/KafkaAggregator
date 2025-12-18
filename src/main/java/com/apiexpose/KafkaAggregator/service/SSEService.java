package com.apiexpose.KafkaAggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// This service will manage Server-Sent Events (SSE) connections

@Service
@Slf4j
public class SSEService
{
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /**
     * Create a new SSE emitter for a client connection
     */
    public SseEmitter createEmitter() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE); // No timeout

        emitters.add(emitter);
        log.info("New SSE client connected. Total clients: {}", emitters.size());

        emitter.onCompletion(() -> {
            emitters.remove(emitter);
            log.info("SSE client disconnected. Total clients: {}", emitters.size());
        });

        emitter.onTimeout(() -> {
            emitters.remove(emitter);
            log.warn("SSE client timeout. Total clients: {}", emitters.size());
        });

        emitter.onError(throwable -> {
            emitters.remove(emitter);
            log.error("SSE client error: {}. Total clients: {}",
                    throwable.getMessage(), emitters.size());
        });

        // Send initial connection message
        try {
            emitter.send(SseEmitter.event()
                    .name("connected")
                    .data("Connection established. Waiting for updates..."));
        } catch (IOException e) {
            log.error("Error sending initial SSE message", e);
            emitters.remove(emitter);
        }

        return emitter;
    }

    /**
     * Broadcast a message to all connected SSE clients
     */
    public void broadcastUpdate(String topicName, String location) {
        String message = String.format("Hi, %s is sending the data from %s",
                topicName, location);

        log.debug("Broadcasting update to {} clients: {}", emitters.size(), message);

        List<SseEmitter> deadEmitters = new CopyOnWriteArrayList<>();

        emitters.forEach(emitter -> {
            try {
                emitter.send(SseEmitter.event()
                        .name("update")
                        .data(message));
            } catch (IOException e) {
                log.error("Error sending SSE update, marking emitter for removal", e);
                deadEmitters.add(emitter);
            }
        });

        // Remove dead emitters
        emitters.removeAll(deadEmitters);

        if (!deadEmitters.isEmpty()) {
            log.info("Removed {} dead SSE connections. Active clients: {}",
                    deadEmitters.size(), emitters.size());
        }
    }

    /**
     * Get the number of active SSE connections
     */
    public int getActiveConnectionsCount() {
        return emitters.size();
    }
}
