package com.local.stockselector.results;

import java.util.List;
import java.util.Map;

public final class ResultModels {
    private ResultModels() {
    }

    public record ApiError(String code, String message, String path) {
    }

    public record SelectionSnapshotResponse(
        String tradeDate,
        String rebalanceMode,
        Integer selectedCount,
        Integer topN,
        Integer stockCount,
        Double avgTotalScore,
        Double maxTotalScore,
        Double minTotalScore,
        List<Map<String, Object>> topStocks,
        String objectKey,
        String createdAt
    ) {
    }

    public record SelectionSnapshotListResponse(List<SelectionSnapshotListItem> items, int limit, int offset) {
    }

    public record SelectionSnapshotListItem(
        String tradeDate,
        Integer topN,
        Integer stockCount,
        Double avgTotalScore,
        Double maxTotalScore,
        Double minTotalScore,
        String objectKey,
        List<SelectionTopStock> topStocks,
        String createdAt
    ) {
    }

    public record SelectionTopStock(String stockCode, Integer rank, Double totalScore) {
    }

    public record BacktestSummaryResponse(
        String runKey,
        String strategyName,
        String startDate,
        String endDate,
        String rebalanceMode,
        Double initialCash,
        Double commissionRate,
        Double slippageBps,
        Double stampTaxRate,
        Integer topN,
        String executionRule,
        String status,
        Map<String, Object> metrics,
        String reportObjectKey,
        String detailObjectKey,
        String createdAt
    ) {
    }

    public record BacktestSummaryListResponse(List<BacktestSummaryListItem> items, int limit, int offset) {
    }

    public record BacktestSummaryListItem(
        String runKey,
        String strategyName,
        String startDate,
        String endDate,
        String rebalanceMode,
        Integer topN,
        String status,
        Double initialCash,
        Double commissionRate,
        Double stampTaxRate,
        Double slippageBps,
        String executionRule,
        Double totalReturn,
        Double annualReturn,
        Double maxDrawdown,
        Double benchmarkHs300Return,
        Double benchmarkCsi500Return,
        Double benchmarkCsi800Return,
        String detailObjectKey,
        String createdAt
    ) {
    }

    public record UpdateLogResponse(String tradeDate, List<UpdateLogEntryResponse> entries) {
    }

    public record UpdateLogEntryResponse(
        String tradeDate,
        String stepName,
        String status,
        String objectKey,
        String message,
        String updatedAt
    ) {
    }

    public record TaskLogResponse(List<TaskLogEntryResponse> items, int limit) {
    }

    public record TaskLogEntryResponse(
        Long id,
        String taskType,
        String status,
        Map<String, Object> params,
        Map<String, Object> resultSummary,
        String objectKey,
        String createdAt,
        String updatedAt
    ) {
    }
}
