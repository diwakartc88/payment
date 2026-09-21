package com.paymentgateway.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling // drives the OutboxPublisher poller
public class PaymentIngestionApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentIngestionApplication.class, args);
    }
}
