package com.paymentgateway.fraud.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Lightweight consumer-side idempotency check (ARCHITECTURE.md §5.2).
 * Kafka gives at-least-once delivery; a rebalance or a retried send can
 * redeliver the same eventId. Redis SETNX is a cheap, fast way to make this
 * stage's side effects (the fraud-checked event it produces) a no-op on
 * redelivery instead of a duplicate.
 *
 * The TTL bounds Redis memory growth and is set well beyond the topic's
 * retention-driven maximum redelivery window.
 */
@Component
@RequiredArgsConstructor
public class IdempotencyGuard {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String KEY_PREFIX = "fraud:processed:";

    private final StringRedisTemplate redisTemplate;

    /** Returns true the FIRST time this eventId is seen; false on every redelivery. */
    public boolean markProcessedIfNew(String eventId) {
        Boolean firstTime = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + eventId, "1", TTL);
        return Boolean.TRUE.equals(firstTime);
    }
}
