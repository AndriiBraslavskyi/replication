package com.de.controllers;

import com.de.services.HttpReplicationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Objects;

@RestController
public class ReplicationController {

    private final HttpReplicationService httpReplicationService;

    public ReplicationController(HttpReplicationService httpReplicationService) {
        this.httpReplicationService = Objects.requireNonNull(httpReplicationService);
    }

    @PostMapping("/messages")
    public Mono<Void> addMessage(@RequestBody String message,
                                    @RequestParam(required = false) Integer replicationConcern) {
        return httpReplicationService.replicateMessage(message, replicationConcern == null ? 0 : replicationConcern);
    }

    @GetMapping("/messages")
    public Mono<Collection<String>> getMessages() {
        return httpReplicationService.getMessages();
    }
}
