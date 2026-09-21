package com.paymentgateway.gateway;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Circuit-breaker fallback: returned when payment-ingestion-service is
 * unavailable or timing out, instead of letting the caller hang or the
 * gateway's own threads pile up.
 */
@RestController
public class FallbackController {

    @RequestMapping("/fallback/payments")
    public Mono<ResponseEntity<Map<String, String>>> paymentsFallback() {
        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of(
                        "status", "UNAVAILABLE",
                        "message", "Payment ingestion is temporarily unavailable. Retry with the same Idempotency-Key."
                )));
    }
}
