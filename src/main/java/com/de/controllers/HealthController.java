package com.de.controllers;

import com.de.services.HealthService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Objects;

@RestController
public class HealthController {

    private final HealthService healthService;

    public HealthController(HealthService healthService) {
        this.healthService = Objects.requireNonNull(healthService);
    }

    @GetMapping(value = "/health", params = "host")
    public Mono<ResponseEntity<String>> getHealth(@RequestParam String host) {
        return Mono.just(healthService.isNodeHealthy(host))
                .map(isHealthy -> ResponseEntity.ok(
                        String.format("Node %s is %s healthy", host, isHealthy ? "" : "not")))
                .onErrorResume(throwable -> Mono.just(ResponseEntity.badRequest().body(throwable.getMessage())));
    }

    @GetMapping("/health")
    public Mono<ResponseEntity<String>> getHealth() {
        return Mono.just(healthService.isHasQuorum())
                .map(isHealthy -> ResponseEntity.ok(String.format("Quorum is %s", isHealthy ? "up" : "down")));
    }
}
