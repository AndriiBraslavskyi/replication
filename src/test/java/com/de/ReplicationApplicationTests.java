package com.de;

import com.de.services.ReplicationService;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static reactor.util.retry.Retry.withThrowable;

class ReplicationApplicationTests {

    private final static Logger logger = LoggerFactory.getLogger(ReplicationService.class);

    @Test
    void contextLoads() throws InterruptedException {
        Scheduler myParallelScheduler = Schedulers.newParallel("my-parallel", 10);
        Scheduler myParallelScheduler2 = Schedulers.newParallel("my-parallel", 10);
        CountDownLatch latch = new CountDownLatch(2);
        Mono.empty()
                .subscribeOn(myParallelScheduler2)
                .doOnNext(s -> logger.info("emmited {}", s))
                .doOnNext(s -> awaitCDL(latch))
                .doOnNext(s -> logger.info("released", s))
                .doOnSubscribe(subscription -> Flux.just("a", "b", "c")
                        .parallel()
                        .runOn(myParallelScheduler)
                        .flatMap(this::emitWithDelay)
                        .doOnNext(s -> countDownLatch(latch))
                        .subscribe())
                .subscribe(integer -> logger.info("yep"));

        Thread.sleep(20000);
    }

    @Test
    void test2() throws InterruptedException {
        Flux.fromIterable(Arrays.asList("a", "b", "c"))
                .parallel()
                .runOn(Schedulers.newParallel("bla", 2))
                .doOnNext(s -> logger.info(s))
                .sequential()
                .doOnComplete(() -> logger.info("finished"))
                .subscribe();

        Thread.sleep(20000);
    }

    @Test
    void test3() throws InterruptedException {

        Mono.just(1)
                .doOnNext(integer -> logger.info("value = {}", integer))
                .map(integer -> {
                    throw new ArrayIndexOutOfBoundsException("illegal");
                })
                .retryWhen(Retry.fixedDelay(999, Duration.ofMillis(2000))
                        .filter(throwable -> throwable instanceof ArrayIndexOutOfBoundsException))
                .retryWhen(Retry.fixedDelay(999, Duration.ofMillis(10000))
                        .filter(throwable -> throwable instanceof IllegalArgumentException))
                .subscribe();

        Thread.sleep(20000);
    }

    private void awaitCDL(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void countDownLatch(CountDownLatch latch) {
        logger.info("latch count down");
        latch.countDown();
    }

    @SneakyThrows
    private Mono<String> emitWithDelay(String letter) {
        logger.info("Current thread is {}", Thread.currentThread().getName());
        if (letter.equals("a")) {
            Thread.sleep(1000);
            return Mono.just(letter);
        } else if (letter.equals("b")) {
            Thread.sleep(5000);
            return Mono.just(letter);
        } else {
            Thread.sleep(10000);
            return Mono.just("c");
        }
    }
}
