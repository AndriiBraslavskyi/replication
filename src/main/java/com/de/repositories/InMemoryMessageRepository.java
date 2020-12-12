package com.de.repositories;

import com.de.model.Message;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryMessageRepository implements MessageRepository {

    final Map<Long, String> messages;

    public InMemoryMessageRepository() {
        this.messages = new ConcurrentSkipListMap<>();
    }

    @Override
    public void persistMessage(Message message) {
        messages.put(message.getId(), message.getPayload());
    }

    @Override
    public Collection<String> readAll() {
        return messages.values();
    }
}
