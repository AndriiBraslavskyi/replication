package com.de.services;

import com.de.model.Message;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

public interface ReplicationService {

    void replicateMessage(String message, int replicationConcern);

    List<Message> getMessages();
}
