package com.de.configurations;

import com.de.repositories.InMemoryMessageRepository;
import com.de.repositories.MessageRepository;
import com.de.services.HealthService;
import com.de.services.HttpReplicationService;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.function.client.WebClient;

import javax.validation.constraints.NotNull;
import java.util.Set;

@Configuration
@EnableScheduling
public class WebConfiguration {

    private final Logger logger = LoggerFactory.getLogger(WebConfiguration.class);

    @Bean
    HttpReplicationService replicationService(ReplicationServiceProperties replicationServiceProperties,
                                              HealthService healthService,
                                              MessageRepository messageRepository,
                                              WebClient webClient) {
        logger.info("Server configuration: {}", replicationServiceProperties);
        return new HttpReplicationService(
                replicationServiceProperties.getHosts(),
                replicationServiceProperties.getRetryNumber(),
                replicationServiceProperties.getTimeout(),
                replicationServiceProperties.getRetryPeriod(),
                healthService,
                messageRepository,
                webClient);
    }

    @Bean
    MessageRepository messageRepository() {
        return new InMemoryMessageRepository();
    }

    @Bean
    WebClient webClient() {
        return WebClient.create();
    }

    @Bean
    ReplicationServiceProperties replicationServiceProperties() {
        return new ReplicationServiceProperties();
    }

    @Bean
    HealthService healthService(@Value("${replicas.hosts}") Set<String> hosts,
                                @Value("${health.timeout}") Long timeout,
                                @Value("${health.nodes-for-quorum}") Integer nodesForQuorum,
                                @Value("${health.crashed-ping-number}") Integer crashedPingNumber,
                                WebClient webClient) {
        return new HealthService(hosts, timeout, nodesForQuorum, crashedPingNumber, webClient);
    }

    @ConfigurationProperties(prefix = "replicas")
    @Data
    @NoArgsConstructor
    private static class ReplicationServiceProperties {
        @NotNull
        Set<String> hosts;

        @NotNull
        Integer retryNumber;

        @NotNull
        Integer timeout;

        @NotNull
        Integer retryPeriod;
    }
}
