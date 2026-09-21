package com.paymentgateway.settlement.repository;

import com.paymentgateway.settlement.domain.SettlementItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface SettlementItemRepository extends JpaRepository<SettlementItem, Long> {

    @Query("SELECT DISTINCT s.merchantId FROM SettlementItem s WHERE s.settled = false")
    List<String> findDistinctMerchantIdsWithUnsettledItems();

    List<SettlementItem> findByMerchantIdAndSettledFalse(String merchantId);
}
