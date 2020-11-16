package com.de.repositories;

import com.de.model.Message;

import java.util.ArrayList;
import java.util.List;

public class InMemoryMessageRepository implements MessageRepository {

    final List<Message> messages;

    public InMemoryMessageRepository() {
        this.messages = new ArrayList<>();
    }

    @Override
    public void persistMessage(Message message) {
        messages.add(message);
    }

    @Override
    public List<Message> readAll() {
        return messages;
    }
}
