package com.paymentgateway.notification.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Stands in for a real merchant-webhook / cardholder-notification delivery
 * system (HTTP POST with HMAC signing + retry/backoff to a merchant's
 * registered endpoint, or an email/SMS/push provider). Logging here keeps
 * the reference pipeline runnable without needing real webhook receivers.
 *
 * A production version of this class is where you'd add: HMAC request
 * signing so merchants can verify authenticity, a bounded retry policy with
 * a dead-letter queue of undeliverable webhooks, and per-merchant delivery
 * status tracking (for a merchant-facing "resend webhook" feature).
 */
@Slf4j
@Component
public class WebhookDispatcher {

    public void dispatch(String eventType, String merchantId, String paymentId, Object payloadSummary) {
        log.info("[WEBHOOK] -> merchantId={} eventType={} paymentId={} payload={}",
                merchantId, eventType, paymentId, payloadSummary);
    }
}
