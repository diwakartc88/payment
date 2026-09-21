package com.paymentgateway.ingestion.repository;

import com.paymentgateway.ingestion.domain.OutboxEvent;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    // At scale this becomes "SELECT ... FOR UPDATE SKIP LOCKED" (or is
    // replaced entirely by CDC) so multiple ingestion pods can poll the same
    // table concurrently without racing each other. See SCALING.md.
    List<OutboxEvent> findByPublishedAtIsNullOrderByCreatedAtAsc(Limit limit);
}
