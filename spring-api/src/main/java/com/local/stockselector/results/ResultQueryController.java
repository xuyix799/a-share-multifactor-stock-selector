package com.local.stockselector.results;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class ResultQueryController {
    private final ResultQueryRepository repository;

    public ResultQueryController(ResultQueryRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/selections")
    ResultModels.SelectionSnapshotListResponse selections(
        @RequestParam(required = false) Integer limit,
        @RequestParam(required = false) Integer offset
    ) {
        int validLimit = RequestValidators.pageLimit(limit);
        int validOffset = RequestValidators.pageOffset(offset);
        List<ResultModels.SelectionSnapshotListItem> items = repository.listSelectionSnapshots(validLimit, validOffset);
        return new ResultModels.SelectionSnapshotListResponse(items, validLimit, validOffset);
    }

    @GetMapping("/selections/{tradeDate}")
    ResultModels.SelectionSnapshotResponse selection(@PathVariable String tradeDate) {
        String validTradeDate = RequestValidators.tradeDate(tradeDate);
        return repository.findSelectionSnapshot(validTradeDate)
            .orElseThrow(() -> new ResourceNotFoundException("selection snapshot not found"));
    }

    @GetMapping("/backtests")
    ResultModels.BacktestSummaryListResponse backtests(
        @RequestParam(required = false) String status,
        @RequestParam(required = false) String rebalanceMode,
        @RequestParam(required = false) Integer limit,
        @RequestParam(required = false) Integer offset
    ) {
        String validStatus = RequestValidators.taskStatus(status);
        String validRebalanceMode = RequestValidators.rebalanceMode(rebalanceMode);
        int validLimit = RequestValidators.pageLimit(limit);
        int validOffset = RequestValidators.pageOffset(offset);
        List<ResultModels.BacktestSummaryListItem> items = repository.listBacktestSummaries(validStatus, validRebalanceMode, validLimit, validOffset);
        return new ResultModels.BacktestSummaryListResponse(items, validLimit, validOffset);
    }

    @GetMapping("/backtests/{runKey}")
    ResultModels.BacktestSummaryResponse backtest(@PathVariable String runKey) {
        String validRunKey = RequestValidators.runKey(runKey);
        return repository.findBacktestSummary(validRunKey)
            .orElseThrow(() -> new ResourceNotFoundException("backtest summary not found"));
    }

    @GetMapping("/update-logs/{tradeDate}")
    ResultModels.UpdateLogResponse updateLogs(@PathVariable String tradeDate) {
        String validTradeDate = RequestValidators.tradeDate(tradeDate);
        List<ResultModels.UpdateLogEntryResponse> entries = repository.listUpdateLogs(validTradeDate);
        return new ResultModels.UpdateLogResponse(validTradeDate, entries);
    }

    @GetMapping("/task-logs")
    ResultModels.TaskLogResponse taskLogs(
        @RequestParam(required = false) String status,
        @RequestParam(required = false) String taskType,
        @RequestParam(required = false) Integer limit
    ) {
        String validStatus = RequestValidators.taskStatus(status);
        String validTaskType = RequestValidators.taskType(taskType);
        int validLimit = RequestValidators.taskLogLimit(limit);
        List<ResultModels.TaskLogEntryResponse> items = repository.listTaskLogs(validStatus, validTaskType, validLimit);
        return new ResultModels.TaskLogResponse(items, validLimit);
    }
}
