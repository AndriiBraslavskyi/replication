package com.de.controllers;

import com.de.model.Message;
import com.de.services.HttpReplicationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Objects;
import java.util.Set;

@RestController
public class ReplicationController {

    private final HttpReplicationService httpReplicationService;

    public ReplicationController(HttpReplicationService httpReplicationService) {
        this.httpReplicationService = Objects.requireNonNull(httpReplicationService);
    }

    @PostMapping("/messages")
    public void addMessage(@RequestBody String message,
                           @RequestParam(required = false) Integer replicationConcern) {
        httpReplicationService.replicateMessage(message, replicationConcern == null ? 0 : replicationConcern);
    }

    @GetMapping("/messages")
    public List<Message> getMessages() {
        return httpReplicationService.getMessages();
    }
}
