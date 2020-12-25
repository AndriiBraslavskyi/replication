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
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HttpReplicationService implements ReplicationService {

    private final static Logger logger = LoggerFactory.getLogger(ReplicationService.class);
    private final static String REPLICATION_PATH = "/messages";
    private final static String LOCAL_ADDRESS = "local";
    private final static int RETRY_THREADS_NUMBER = 50;
    private final static int RESPONSE_THREADS_NUMBER = 50;

    private final Set<String> endpoints;
    private final int retryNumber;
    private final int timeout;
    private final int retryPeriod;
    private final MessageRepository messageRepository;
    private final WebClient webClient;
    private final Scheduler retryScheduler;
    private final Scheduler responseScheduler;
    private final AtomicLong messageClock;

    public HttpReplicationService(Set<String> replicasHosts,
                                  Integer retryNumber,
                                  Integer timeout,
                                  Integer retryPeriod,
                                  MessageRepository messageRepository,
                                  WebClient webClient) {
        this.endpoints = makeEndpoints(replicasHosts);
        this.retryNumber = Objects.requireNonNull(retryNumber);
        this.timeout = Objects.requireNonNull(timeout);
        this.retryPeriod = Objects.requireNonNull(retryPeriod);
        this.messageRepository = Objects.requireNonNull(messageRepository);
        this.webClient = Objects.requireNonNull(webClient);
        this.retryScheduler = Schedulers.newParallel("retry-parallel", RETRY_THREADS_NUMBER);
        this.responseScheduler = Schedulers.newParallel("response-parallel", RESPONSE_THREADS_NUMBER);
        this.messageClock = new AtomicLong();
    }

    private Set<String> makeEndpoints(Set<String> replicasHosts) {
        return Objects.requireNonNull(replicasHosts).stream()
                .map(host -> host + REPLICATION_PATH)
                .collect(Collectors.toSet());
    }

    public Mono<Void> replicateMessage(String payload, int replicationConcern) {
        final Message message = Message.of(payload, messageClock.incrementAndGet());
        logger.info("Sending message {} with concern {}", message, replicationConcern);
        if (replicationConcern > endpoints.size() + 1) {
            throw new InvalidRequestException(String.format("Failed to replicate message `%s`, reason: "
                            + "replication concern parameter `%d` exceeded hosts number `%d`",
                    message, replicationConcern, endpoints.size()));
        }
        final CountDownLatch countDownLatch = new CountDownLatch(replicationConcern);

        return Mono.just(1)
                .subscribeOn(responseScheduler)
                .doOnNext(s -> awaitOnCountDownLatch(countDownLatch))
                .doOnSubscribe(ignored -> Flux.concat(Mono.just(LOCAL_ADDRESS), Flux.fromIterable(endpoints))
                        .parallel()
                        .runOn(retryScheduler)
                        .flatMap(host -> persistReplica(message, host, retryNumber, timeout))
                        .doOnNext(unused -> countDownLatch.countDown())
                        .doOnNext(unused -> awaitOnCountDownLatch(countDownLatch))
                        .subscribe())
                .then();
    }

    private Mono<ClientResponse> persistReplica(Message message, String host, int retryNumber, int timeout) {
        if (host.equals(LOCAL_ADDRESS)) {
            return saveReplicaToLocally(message);
        } else {
            return sendReplicaToRemoteHost(message, host, retryNumber, timeout);
        }
    }

    private Mono<ClientResponse> saveReplicaToLocally(Message message) {
        messageRepository.persistMessage(message);
        return Mono.just(ClientResponse.create(HttpStatus.OK).build());
    }

    private Mono<ClientResponse> sendReplicaToRemoteHost(Message message, String host, int retryNumber, int timeout) {
        return webClient.post()
                        .uri(host)
                        .body(Mono.just(message), Message.class)
                        .exchange()
                        .timeout(Duration.ofMillis(timeout),
                                Mono.just(ClientResponse.create(HttpStatus.REQUEST_TIMEOUT).build()))
                        .flatMap(clientResponse -> errorIfInternalError(message, clientResponse))
                        .retryWhen(Retry.fixedDelay(retryNumber, Duration.ofMillis(retryPeriod))
                        .filter(Objects::nonNull));
    }

    private void awaitOnCountDownLatch(CountDownLatch countDownLatch) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new InternalServerException("Failed while waiting for replication concern number to respond");
        }
    }

    private Mono<ClientResponse> errorIfInternalError(Message message, ClientResponse clientResponse) {
        final HttpStatus statusCode = clientResponse.statusCode();
        logger.info("Status = {}, message = {}", statusCode.value(), message);
        if ((statusCode.is5xxServerError() || statusCode.is4xxClientError())
                && statusCode.value() != HttpStatus.CONFLICT.value()) {
            return Mono.error(new InternalServerException("Failed to call service"));
        } else {
            return Mono.just(clientResponse);
        }
    }

    public Mono<Collection<String>> getMessages() {
        return messageRepository.readAll();
    }
}
