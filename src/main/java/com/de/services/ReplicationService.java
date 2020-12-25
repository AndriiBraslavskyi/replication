package com.de.services;

import reactor.core.publisher.Mono;

import java.util.Collection;

public interface ReplicationService {

    Mono<Void> replicateMessage(String message, int replicationConcern);

    Mono<Collection<String>> getMessages();
}
