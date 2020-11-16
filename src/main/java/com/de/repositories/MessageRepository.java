package com.de.repositories;

import com.de.model.Message;

import java.util.List;

public interface MessageRepository {
    void persistMessage(Message message);

    List<Message> readAll();
}
