package com.paymentgateway.gateway;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Supplies the bucket key for the RequestRateLimiter filter. Without this bean
 * Spring Cloud Gateway falls back to PrincipalNameKeyResolver, which yields an
 * empty key on these unauthenticated calls and rejects every request with 403
 * (deny-empty-key defaults to true).
 *
 * Keyed per merchant API key so one noisy merchant cannot spend another's
 * budget, falling back to the caller's address for unkeyed traffic. A
 * production build resolves this from the authenticated API-key principal
 * once auth lands at the edge.
 */
@Configuration
public class RateLimiterConfig {

    @Bean
    public KeyResolver apiKeyResolver() {
        return exchange -> {
            String apiKey = exchange.getRequest().getHeaders().getFirst("X-API-Key");
            if (apiKey != null && !apiKey.isBlank()) {
                return Mono.just(apiKey);
            }
            var remote = exchange.getRequest().getRemoteAddress();
            return Mono.just(remote != null ? remote.getAddress().getHostAddress() : "unknown");
        };
    }
}
