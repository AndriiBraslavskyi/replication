package com.de.repositories;

import com.de.model.Message;

import java.util.Collection;

public interface MessageRepository {
    void persistMessage(Message message);

    Collection<String> readAll();
}
