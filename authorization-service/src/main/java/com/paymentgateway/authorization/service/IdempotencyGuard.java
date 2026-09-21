package com.paymentgateway.authorization.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class IdempotencyGuard {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String KEY_PREFIX = "authorization:processed:";

    private final StringRedisTemplate redisTemplate;

    public boolean markProcessedIfNew(String eventId) {
        Boolean firstTime = redisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + eventId, "1", TTL);
        return Boolean.TRUE.equals(firstTime);
    }
}
