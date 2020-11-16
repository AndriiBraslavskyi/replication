package com.de.services;

import com.de.exceptions.InternalServerException;
import com.de.exceptions.InvalidRequestException;
import com.de.model.Message;
import com.de.repositories.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class HttpReplicationService implements ReplicationService {

    private final static Logger logger = LoggerFactory.getLogger(ReplicationService.class);
    private final static String REPLICATION_PATH = "/messages";

    private final Set<String> endpoints;
    private final int retryNumber;
    private final int timeout;
    private final MessageRepository messageRepository;
    private final WebClient webClient;
    private final Scheduler parallelScheduler;

    public HttpReplicationService(Set<String> replicasHosts,
                                  Integer retryNumber,
                                  Integer timeout,
                                  MessageRepository messageRepository,
                                  WebClient webClient) {
        this.endpoints = makeEndpoints(replicasHosts);
        this.retryNumber = Objects.requireNonNull(retryNumber);
        this.timeout = Objects.requireNonNull(timeout);
        this.messageRepository = Objects.requireNonNull(messageRepository);
        this.webClient = Objects.requireNonNull(webClient);
        this.parallelScheduler = Schedulers.parallel();
    }

    private Set<String> makeEndpoints(Set<String> replicasHosts) {
        return Objects.requireNonNull(replicasHosts).stream()
                .map(host -> host + REPLICATION_PATH)
                .collect(Collectors.toSet());
    }

    public void replicateMessage(String payload, int replicationConcern) {
        final Message message = Message.of(payload, UUID.randomUUID().toString());
        logger.info("Sending message {}", message);
        if (replicationConcern > endpoints.size()) {
            throw new InvalidRequestException(String.format("Failed to replicate message `%s`, reason: "
                            + "replication concern parameter `%d` exceeded hosts number `%d`",
                    message, replicationConcern, endpoints.size()));
        }
        final CountDownLatch countDownLatch = new CountDownLatch(replicationConcern);

        messageRepository.persistMessage(message);
        Flux.fromIterable(endpoints)
                .parallel()
                .runOn(parallelScheduler)
                .flatMap(host -> sendReplica(message, host, retryNumber, timeout))
                .doOnNext(clientResponse -> countDownLatch.countDown())
                .subscribe();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new InternalServerException("Failed while waiting for replication concern number to respond");
        }
    }

    private Mono<ClientResponse> sendReplica(Message message, String host, int retryNumber, int timeout) {
        return webClient.post()
                .uri(host)
                .body(Mono.just(message), Message.class)
                .exchange()
                .timeout(Duration.ofMillis(timeout),
                        Mono.just(ClientResponse.create(HttpStatus.REQUEST_TIMEOUT).build()))
                .flatMap(clientResponse -> errorIfInternalError(message, clientResponse))
                .retryWhen(Retry.fixedDelay(retryNumber, Duration.ofSeconds(1))
                        .filter(Objects::nonNull));
    }

    private Mono<ClientResponse> errorIfInternalError(Message message, ClientResponse clientResponse) {
        final HttpStatus statusCode = clientResponse.statusCode();
        logger.info("Status = {}, message = {}", statusCode.value(), message);
        if (statusCode.is5xxServerError() || statusCode.is4xxClientError()) {
            return Mono.error(new InternalServerException("Failed to call service"));
        } else {
            return Mono.just(clientResponse);
        }
    }

    public List<Message> getMessages() {
        return messageRepository.readAll();
    }
}
