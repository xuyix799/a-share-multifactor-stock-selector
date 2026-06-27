package com.local.stockselector.results;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ResultQueryController.class)
class ResultQueryControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ResultQueryRepository repository;

    @BeforeEach
    void resetRepository() {
        reset(repository);
    }

    @Test
    void selectionByTradeDateReturnsSnapshotAndObjectKey() throws Exception {
        var response = new ResultModels.SelectionSnapshotResponse(
            "2026-06-19",
            "daily",
            2,
            50,
            2,
            86.5,
            91.0,
            82.0,
            List.of(Map.of("stock_code", "000001.SZ", "rank", 1, "total_score", 91.0)),
            "processed/selection_result/trade_date=2026-06-19/part.parquet",
            "2026-06-19T10:00:00Z"
        );
        when(repository.findSelectionSnapshot("2026-06-19")).thenReturn(Optional.of(response));

        mockMvc.perform(get("/api/selections/2026-06-19"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.tradeDate").value("2026-06-19"))
            .andExpect(jsonPath("$.objectKey").value("processed/selection_result/trade_date=2026-06-19/part.parquet"))
            .andExpect(jsonPath("$.topStocks[0].stock_code").value("000001.SZ"));

        verify(repository).findSelectionSnapshot("2026-06-19");
    }

    @Test
    void selectionsListReturnsPagedSnapshots() throws Exception {
        var response = new ResultModels.SelectionSnapshotListResponse(
            List.of(new ResultModels.SelectionSnapshotListItem(
                "2026-06-19",
                50,
                1,
                72.5,
                72.5,
                72.5,
                "processed/selection_result/trade_date=2026-06-19/part.parquet",
                List.of(new ResultModels.SelectionTopStock("000001.SZ", 1, 72.5)),
                "2026-06-19T10:00:00Z"
            )),
            20,
            0
        );
        when(repository.listSelectionSnapshots(20, 0)).thenReturn(response.items());

        mockMvc.perform(get("/api/selections"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.limit").value(20))
            .andExpect(jsonPath("$.offset").value(0))
            .andExpect(jsonPath("$.items[0].tradeDate").value("2026-06-19"))
            .andExpect(jsonPath("$.items[0].objectKey").value("processed/selection_result/trade_date=2026-06-19/part.parquet"))
            .andExpect(jsonPath("$.items[0].topStocks[0].stockCode").value("000001.SZ"))
            .andExpect(jsonPath("$.items[0].topStocks[0].totalScore").value(72.5));

        verify(repository).listSelectionSnapshots(20, 0);
    }

    @Test
    void backtestByRunKeyReturnsSummaryAndDetailObjectKey() throws Exception {
        var response = new ResultModels.BacktestSummaryResponse(
            "0123456789abcdef",
            "mid_long_mock",
            "2026-01-01",
            "2026-06-30",
            "monthly",
            1000000.0,
            0.0003,
            5.0,
            0.001,
            50,
            "next_open",
            "done",
            Map.of("annual_return", 0.12, "max_drawdown", -0.08),
            null,
            "backtest/detail/run_key=0123456789abcdef/part.parquet",
            "2026-06-19T10:00:00Z"
        );
        when(repository.findBacktestSummary("0123456789abcdef")).thenReturn(Optional.of(response));

        mockMvc.perform(get("/api/backtests/0123456789abcdef"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.runKey").value("0123456789abcdef"))
            .andExpect(jsonPath("$.metrics.annual_return").value(0.12))
            .andExpect(jsonPath("$.detailObjectKey").value("backtest/detail/run_key=0123456789abcdef/part.parquet"));

        verify(repository).findBacktestSummary("0123456789abcdef");
    }

    @Test
    void backtestsListReturnsPagedSummariesWithFlattenedMetrics() throws Exception {
        var response = new ResultModels.BacktestSummaryListResponse(
            List.of(new ResultModels.BacktestSummaryListItem(
                "0123456789abcdef",
                "mid_long_mock",
                "2026-01-01",
                "2026-06-30",
                "monthly",
                50,
                "done",
                1000000.0,
                0.0003,
                0.001,
                5.0,
                "next_open",
                0.12,
                0.18,
                -0.08,
                0.05,
                0.04,
                0.06,
                "backtest/detail/run_key=0123456789abcdef/part.parquet",
                "2026-06-19T10:00:00Z"
            )),
            10,
            5
        );
        when(repository.listBacktestSummaries("done", "monthly", 10, 5)).thenReturn(response.items());

        mockMvc.perform(get("/api/backtests")
                .param("status", "done")
                .param("rebalanceMode", "monthly")
                .param("limit", "10")
                .param("offset", "5"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.limit").value(10))
            .andExpect(jsonPath("$.offset").value(5))
            .andExpect(jsonPath("$.items[0].runKey").value("0123456789abcdef"))
            .andExpect(jsonPath("$.items[0].totalReturn").value(0.12))
            .andExpect(jsonPath("$.items[0].annualReturn").value(0.18))
            .andExpect(jsonPath("$.items[0].benchmarkHs300Return").value(0.05))
            .andExpect(jsonPath("$.items[0].detailObjectKey").value("backtest/detail/run_key=0123456789abcdef/part.parquet"));

        verify(repository).listBacktestSummaries("done", "monthly", 10, 5);
    }

    @Test
    void updateLogsByTradeDateReturnEntries() throws Exception {
        when(repository.listUpdateLogs("2026-06-19")).thenReturn(List.of(
            new ResultModels.UpdateLogEntryResponse(
                "2026-06-19",
                "scoring:selection_result",
                "done",
                "processed/selection_result/trade_date=2026-06-19/part.parquet",
                null,
                "2026-06-19T10:00:00Z"
            )
        ));

        mockMvc.perform(get("/api/update-logs/2026-06-19"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.tradeDate").value("2026-06-19"))
            .andExpect(jsonPath("$.entries[0].stepName").value("scoring:selection_result"))
            .andExpect(jsonPath("$.entries[0].objectKey").value("processed/selection_result/trade_date=2026-06-19/part.parquet"));

        verify(repository).listUpdateLogs("2026-06-19");
    }

    @Test
    void taskLogsCanBeFilteredByStatusTaskTypeAndLimit() throws Exception {
        when(repository.listTaskLogs("done", "backtest", 10)).thenReturn(List.of(
            new ResultModels.TaskLogEntryResponse(
                7L,
                "backtest",
                "done",
                Map.of("run_key", "0123456789abcdef"),
                Map.of("detail_object_key", "backtest/detail/run_key=0123456789abcdef/part.parquet"),
                "backtest/detail/run_key=0123456789abcdef/part.parquet",
                "2026-06-19T10:00:00Z",
                "2026-06-19T10:00:00Z"
            )
        ));

        mockMvc.perform(get("/api/task-logs").param("status", "done").param("taskType", "backtest").param("limit", "10"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.limit").value(10))
            .andExpect(jsonPath("$.items[0].taskType").value("backtest"))
            .andExpect(jsonPath("$.items[0].objectKey").value("backtest/detail/run_key=0123456789abcdef/part.parquet"));

        verify(repository).listTaskLogs("done", "backtest", 10);
    }

    @Test
    void selectionsListRejectsInvalidLimit() throws Exception {
        mockMvc.perform(get("/api/selections").param("limit", "101"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        verifyNoInteractions(repository);
    }

    @Test
    void backtestsListRejectsInvalidStatus() throws Exception {
        mockMvc.perform(get("/api/backtests").param("status", "cancelled"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        verifyNoInteractions(repository);
    }

    @Test
    void backtestsListRejectsInvalidRebalanceMode() throws Exception {
        mockMvc.perform(get("/api/backtests").param("rebalanceMode", "weekly"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        verifyNoInteractions(repository);
    }

    @Test
    void listDatabaseErrorsReturnSanitizedInternalError() throws Exception {
        doThrow(new DataAccessResourceFailureException(
            "jdbc:postgresql://stock-postgres:5432/stock_selector password=secret stackTrace Exception"
        )).when(repository).listBacktestSummaries(null, null, 20, 0);

        mockMvc.perform(get("/api/backtests"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"))
            .andExpect(jsonPath("$.message").value("Internal server error"))
            .andExpect(content().string(not(containsString("password"))))
            .andExpect(content().string(not(containsString("jdbc"))))
            .andExpect(content().string(not(containsString("stackTrace"))))
            .andExpect(content().string(not(containsString("Exception"))));
    }

    @Test
    void invalidTradeDateReturnsBadRequestBeforeRepositoryCall() throws Exception {
        mockMvc.perform(get("/api/selections/2026-02-31"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
            .andExpect(jsonPath("$.path").value("/api/selections/2026-02-31"));

        verifyNoInteractions(repository);
    }

    @Test
    void missingSelectionReturnsNotFoundForValidDate() throws Exception {
        when(repository.findSelectionSnapshot("2026-06-20")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/selections/2026-06-20"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.code").value("NOT_FOUND"));

        verify(repository).findSelectionSnapshot("2026-06-20");
    }

    @Test
    void invalidRunKeyReturnsBadRequestBeforeRepositoryCall() throws Exception {
        mockMvc.perform(get("/api/backtests/not-safe"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        verifyNoInteractions(repository);
    }

    @Test
    void missingBacktestReturnsNotFoundForValidRunKey() throws Exception {
        when(repository.findBacktestSummary("fedcba9876543210")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/backtests/fedcba9876543210"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.code").value("NOT_FOUND"));

        verify(repository).findBacktestSummary("fedcba9876543210");
    }

    @Test
    void databaseErrorsReturnSanitizedInternalError() throws Exception {
        doThrow(new DataAccessResourceFailureException(
            "jdbc:postgresql://stock-postgres:5432/stock_selector password=secret stackTrace Exception"
        )).when(repository).findSelectionSnapshot("2026-06-19");

        mockMvc.perform(get("/api/selections/2026-06-19"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"))
            .andExpect(jsonPath("$.message").value("Internal server error"))
            .andExpect(content().string(not(containsString("password"))))
            .andExpect(content().string(not(containsString("jdbc"))))
            .andExpect(content().string(not(containsString("stackTrace"))))
            .andExpect(content().string(not(containsString("Exception"))));
    }

    @TestConfiguration
    static class MockRepositoryConfig {
        @Bean
        ResultQueryRepository resultQueryRepository() {
            return Mockito.mock(ResultQueryRepository.class);
        }
    }
}
