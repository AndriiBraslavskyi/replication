package com.de.services;

import java.util.Collection;

public interface ReplicationService {

    void replicateMessage(String message, int replicationConcern);

    Collection<String> getMessages();
}
