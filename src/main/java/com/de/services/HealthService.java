package com.de.services;

import com.de.exceptions.InvalidRequestException;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HealthService {

    private final static Logger logger = LoggerFactory.getLogger(HealthService.class);
    private final static String HEALTH_ENDPOINT_SUFFIX = "/health";
    private static final String HEALTH = "health";

    private final Set<String> endpoints;
    private final long healthTimeout;
    private final int nodesForQuorum;
    private final WebClient webClient;
    private final Scheduler healthScheduler;

    private final Map<String, Boolean> nodesHealth;
    private volatile boolean hasQuorum;

    public HealthService(Set<String> endpoints, Long healthTimeout, Integer nodesForQuorum, WebClient webClient) {
        this.endpoints = endpoints;
        this.healthTimeout = Objects.requireNonNull(healthTimeout);
        this.nodesForQuorum = Objects.requireNonNull(nodesForQuorum);
        this.webClient = Objects.requireNonNull(webClient);
        this.nodesHealth = initializeNodeHealth(endpoints);
        this.hasQuorum = true;
        this.healthScheduler = Schedulers.newParallel(HEALTH, endpoints.size());
    }

    public boolean isHasQuorum() {
        return hasQuorum;
    }

    public boolean isNodeHealthy(String endpoint) {
        final Boolean isNodeHealthy = nodesHealth.get(endpoint);
        if (isNodeHealthy == null) {
            throw new InvalidRequestException(String.format("Node %s was not found", endpoint));
        } else {
            return isNodeHealthy;
        }
    }

    private Map<String, Boolean> initializeNodeHealth(Set<String> endpoints) {
        return CollectionUtils.emptyIfNull(endpoints).stream()
                .collect(Collectors.toMap(Function.identity(), s -> true));
    }

    @Scheduled(cron = "${health.period-ms}")
    private void checkHealth() {
        Flux.fromIterable(endpoints)
                .parallel()
                .runOn(healthScheduler)
                .flatMap(this::getSecondaryNodeHealth)
                .sequential()
                .doOnComplete(this::updateQuorum)
                .doOnError(throwable -> updateQuorum())
                .subscribe();
    }

    private Mono<ClientResponse> getSecondaryNodeHealth(String endpoint) {
        return webClient.get()
                .uri(buildHealthEndpoint(endpoint))
                .exchange()
                .publishOn(healthScheduler)
                .timeout(Duration.ofMillis(healthTimeout),
                        Mono.just(ClientResponse.create(HttpStatus.REQUEST_TIMEOUT).build()))
                .doOnNext(clientResponse -> setNodeHealth(clientResponse, endpoint))
                .doOnError(throwable -> updateNodeAsDead(endpoint));
    }

    private String buildHealthEndpoint(String endpoint) {
        return endpoint + HEALTH_ENDPOINT_SUFFIX;
    }

    private void setNodeHealth(ClientResponse clientResponse, String url) {
        final HttpStatus httpStatus = clientResponse.statusCode();
        if (httpStatus != HttpStatus.OK) {
            nodesHealth.put(url, false);
        } else {
            nodesHealth.put(url, true);
        }
    }

    private void updateNodeAsDead(String url) {
        nodesHealth.put(url, false);
    }

    private void updateQuorum() {
        final int liveNodes = nodesHealth.values().stream()
                .filter(aBoolean -> aBoolean)
                .mapToInt(value -> 1)
                .sum();
        hasQuorum = liveNodes >= nodesForQuorum;
        logger.info("Current node states are: {}", nodesHealth);
    }
}
