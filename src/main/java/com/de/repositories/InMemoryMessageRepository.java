package com.de.repositories;

import com.de.model.Message;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryMessageRepository implements MessageRepository {

    final Map<Long, String> messages;

    public InMemoryMessageRepository() {
        this.messages = new ConcurrentSkipListMap<>();
    }

    @Override
    public Mono<Void> persistMessage(Message message) {
        messages.put(message.getId(), message.getPayload());
        return Mono.empty();
    }

    @Override
    public Mono<Collection<String>> readAll() {
        return Mono.just(messages.values());
    }
}
