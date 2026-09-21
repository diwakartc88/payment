package com.paymentgateway.fraud.service;

import com.paymentgateway.events.FraudDecision;
import com.paymentgateway.events.PaymentInitiatedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Deliberately simple, deterministic rules -- illustrates where a real fraud
 * engine plugs in (velocity limits + a hard amount ceiling here; a real
 * system adds a scored ML model call, device fingerprinting, a merchant
 * risk tier, etc., all fanned out from this same consumed event).
 *
 * This is the stage most likely to be CPU/latency-bound in production
 * (rule evaluation, possibly a model inference call) -- it's called out in
 * ARCHITECTURE.md §2 as the stage that typically needs the most partitions
 * and consumer replicas relative to the others.
 */
@Component
@RequiredArgsConstructor
public class FraudRuleEngine {

    private static final Duration VELOCITY_WINDOW = Duration.ofMinutes(1);
    private static final String VELOCITY_KEY_PREFIX = "fraud:velocity:";

    @Value("${fraud.rules.max-single-amount:10000}")
    private BigDecimal maxSingleAmount;

    @Value("${fraud.rules.max-transactions-per-minute:20}")
    private long maxTransactionsPerMinute;

    private final StringRedisTemplate redisTemplate;

    public Decision evaluate(PaymentInitiatedEvent event) {
        if (event.getAmount().compareTo(maxSingleAmount) > 0) {
            return new Decision(FraudDecision.MANUAL_REVIEW,
                    "amount " + event.getAmount() + " " + event.getCurrency() + " exceeds single-transaction threshold");
        }

        long countInWindow = incrementAndGetVelocity(event.getAccountId());
        if (countInWindow > maxTransactionsPerMinute) {
            return new Decision(FraudDecision.DECLINED,
                    "velocity check failed: " + countInWindow + " transactions in the last minute for accountId=" + event.getAccountId());
        }

        return new Decision(FraudDecision.APPROVED, null);
    }

    private long incrementAndGetVelocity(String accountId) {
        String key = VELOCITY_KEY_PREFIX + accountId;
        Long count = redisTemplate.opsForValue().increment(key);
        if (count != null && count == 1L) {
            redisTemplate.expire(key, VELOCITY_WINDOW);
        }
        return count == null ? 0L : count;
    }

    public record Decision(FraudDecision decision, String reason) {
    }
}
