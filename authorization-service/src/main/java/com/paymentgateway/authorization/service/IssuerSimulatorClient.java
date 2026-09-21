package com.paymentgateway.authorization.service;

import com.paymentgateway.events.AuthorizationStatus;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Stands in for a real issuer / card network integration (ISO 8583 over a
 * dedicated link, or a card network API). Deterministic here so the
 * reference pipeline is reproducible: declines on a fixed "unlucky" amount
 * pattern instead of a real risk decision.
 *
 * The @CircuitBreaker/@Retry annotations are the actual point of this class:
 * a slow or failing issuer must not stall this service's whole consumer
 * group (ARCHITECTURE.md §5.5). See application.yml for the resilience4j
 * threshold configuration and the authorizationFallback below for what
 * happens when the breaker is open.
 */
@Slf4j
@Component
public class IssuerSimulatorClient {

    @CircuitBreaker(name = "issuer", fallbackMethod = "authorizeFallback")
    @Retry(name = "issuer")
    public AuthResult authorize(String paymentId, String accountId, BigDecimal amount) {
        simulateNetworkLatency();

        // Deterministic "decline" for demo purposes: amounts ending in .13
        // are declined by the simulated issuer, everything else approved.
        boolean declined = amount.remainder(BigDecimal.ONE).compareTo(new BigDecimal("0.13")) == 0;

        if (declined) {
            return new AuthResult(AuthorizationStatus.DECLINED, null, "issuer declined: insufficient funds (simulated)");
        }
        return new AuthResult(AuthorizationStatus.AUTHORIZED, "AUTH-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(), null);
    }

    public AuthResult authorizeFallback(String paymentId, String accountId, BigDecimal amount, Throwable t) {
        log.warn("Issuer circuit open/retries exhausted for paymentId={}: {}", paymentId, t.getMessage());
        return new AuthResult(AuthorizationStatus.DECLINED, null, "issuer unavailable (circuit open): " + t.getClass().getSimpleName());
    }

    private void simulateNetworkLatency() {
        try {
            Thread.sleep(15);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public record AuthResult(AuthorizationStatus status, String authorizationCode, String reason) {
    }
}
