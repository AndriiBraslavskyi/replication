package com.de.repositories;

import com.de.model.Message;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface MessageRepository {
    Mono<Void> persistMessage(Message message);

    Mono<Collection<String>> readAll();
}
