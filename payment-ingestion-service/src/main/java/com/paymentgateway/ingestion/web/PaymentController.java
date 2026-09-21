package com.paymentgateway.ingestion.web;

import com.paymentgateway.ingestion.dto.PaymentRequest;
import com.paymentgateway.ingestion.dto.PaymentResponse;
import com.paymentgateway.ingestion.service.PaymentIngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentIngestionService paymentIngestionService;

    @PostMapping
    public ResponseEntity<PaymentResponse> createPayment(
            @Valid @RequestBody PaymentRequest request,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId) {

        String effectiveTraceId = (traceId != null && !traceId.isBlank()) ? traceId : UUID.randomUUID().toString();

        PaymentResponse response = paymentIngestionService.ingest(request, idempotencyKey, effectiveTraceId);

        // A replayed request returns 200 (here's what already happened);
        // a genuinely new payment returns 202 (accepted, processing is async
        // from here on out via Kafka -- there is no synchronous "authorized"
        // answer at this endpoint).
        HttpStatus status = response.replayed() ? HttpStatus.OK : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status)
                .header("X-Trace-Id", effectiveTraceId)
                .body(response);
    }
}
