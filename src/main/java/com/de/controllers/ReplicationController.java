package com.de.controllers;

import com.de.services.HttpReplicationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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

    private final String EMPTY_BODY = "";

    private final HttpReplicationService httpReplicationService;

    public ReplicationController(HttpReplicationService httpReplicationService) {
        this.httpReplicationService = Objects.requireNonNull(httpReplicationService);
    }

    @PostMapping("/messages")
    public Mono<ResponseEntity<String>> addMessage(@RequestBody String message,
                                                   @RequestParam(required = false) Integer replicationConcern) {
        return httpReplicationService.replicateMessage(message, replicationConcern == null ? 0 : replicationConcern)
                .map(unused -> ResponseEntity.status(HttpStatus.OK).body(EMPTY_BODY))
                .onErrorResume(throwable -> Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(throwable.getMessage())));
    }

    @GetMapping("/messages")
    public Mono<Collection<String>> getMessages() {
        return httpReplicationService.getMessages();
    }
}
