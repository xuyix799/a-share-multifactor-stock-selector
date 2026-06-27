package com.local.stockselector.results;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ResultQueryRepository {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAP_TYPE = new TypeReference<>() {
    };

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public ResultQueryRepository(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public Optional<ResultModels.SelectionSnapshotResponse> findSelectionSnapshot(String tradeDate) {
        String sql = """
            SELECT
                trade_date::text AS trade_date,
                rebalance_mode,
                selected_count,
                top_n,
                stock_count,
                avg_total_score,
                max_total_score,
                min_total_score,
                top_stocks::text AS top_stocks,
                object_key,
                created_at::text AS created_at
            FROM selection_snapshot
            WHERE trade_date = CAST(:tradeDate AS DATE)
            ORDER BY created_at DESC
            LIMIT 1
            """;
        MapSqlParameterSource params = new MapSqlParameterSource("tradeDate", tradeDate);
        List<ResultModels.SelectionSnapshotResponse> rows = jdbcTemplate.query(sql, params, this::mapSelectionSnapshot);
        return rows.stream().findFirst();
    }

    public List<ResultModels.SelectionSnapshotListItem> listSelectionSnapshots(int limit, int offset) {
        String sql = """
            SELECT
                trade_date::text AS trade_date,
                top_n,
                stock_count,
                avg_total_score,
                max_total_score,
                min_total_score,
                top_stocks::text AS top_stocks,
                object_key,
                created_at::text AS created_at
            FROM selection_snapshot
            ORDER BY trade_date DESC, created_at DESC, id DESC
            LIMIT :limit OFFSET :offset
            """;
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("offset", offset);
        return jdbcTemplate.query(sql, params, this::mapSelectionSnapshotListItem);
    }

    public Optional<ResultModels.BacktestSummaryResponse> findBacktestSummary(String runKey) {
        String sql = """
            SELECT
                run_key,
                strategy_name,
                start_date::text AS start_date,
                end_date::text AS end_date,
                rebalance_mode,
                initial_cash,
                commission_rate,
                slippage_bps,
                stamp_tax_rate,
                top_n,
                execution_rule,
                status,
                metrics::text AS metrics,
                report_object_key,
                detail_object_key,
                created_at::text AS created_at
            FROM backtest_summary
            WHERE run_key = :runKey
            ORDER BY created_at DESC
            LIMIT 1
            """;
        MapSqlParameterSource params = new MapSqlParameterSource("runKey", runKey);
        List<ResultModels.BacktestSummaryResponse> rows = jdbcTemplate.query(sql, params, this::mapBacktestSummary);
        return rows.stream().findFirst();
    }

    public List<ResultModels.BacktestSummaryListItem> listBacktestSummaries(String status, String rebalanceMode, int limit, int offset) {
        StringBuilder sql = new StringBuilder("""
            SELECT
                run_key,
                strategy_name,
                start_date::text AS start_date,
                end_date::text AS end_date,
                rebalance_mode,
                initial_cash,
                commission_rate,
                slippage_bps,
                stamp_tax_rate,
                top_n,
                execution_rule,
                status,
                metrics::text AS metrics,
                detail_object_key,
                created_at::text AS created_at
            FROM backtest_summary
            WHERE 1 = 1
            """);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("offset", offset);
        if (status != null) {
            sql.append(" AND status = :status");
            params.addValue("status", status);
        }
        if (rebalanceMode != null) {
            sql.append(" AND rebalance_mode = :rebalanceMode");
            params.addValue("rebalanceMode", rebalanceMode);
        }
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT :limit OFFSET :offset");
        return jdbcTemplate.query(sql.toString(), params, this::mapBacktestSummaryListItem);
    }

    public List<ResultModels.UpdateLogEntryResponse> listUpdateLogs(String tradeDate) {
        String sql = """
            SELECT
                trade_date::text AS trade_date,
                step_name,
                status,
                object_key,
                message,
                updated_at::text AS updated_at
            FROM update_log
            WHERE trade_date = CAST(:tradeDate AS DATE)
            ORDER BY step_name
            """;
        MapSqlParameterSource params = new MapSqlParameterSource("tradeDate", tradeDate);
        return jdbcTemplate.query(sql, params, this::mapUpdateLogEntry);
    }

    public List<ResultModels.TaskLogEntryResponse> listTaskLogs(String status, String taskType, int limit) {
        StringBuilder sql = new StringBuilder("""
            SELECT
                id,
                task_type,
                status,
                params::text AS params,
                result_summary::text AS result_summary,
                object_key,
                created_at::text AS created_at,
                updated_at::text AS updated_at
            FROM task_log
            WHERE 1 = 1
            """);
        MapSqlParameterSource params = new MapSqlParameterSource("limit", limit);
        if (status != null) {
            sql.append(" AND status = :status");
            params.addValue("status", status);
        }
        if (taskType != null) {
            sql.append(" AND task_type = :taskType");
            params.addValue("taskType", taskType);
        }
        sql.append(" ORDER BY updated_at DESC, id DESC LIMIT :limit");
        return jdbcTemplate.query(sql.toString(), params, this::mapTaskLogEntry);
    }

    private ResultModels.SelectionSnapshotResponse mapSelectionSnapshot(ResultSet rs, int rowNum) throws SQLException {
        return new ResultModels.SelectionSnapshotResponse(
            rs.getString("trade_date"),
            rs.getString("rebalance_mode"),
            getInteger(rs, "selected_count"),
            getInteger(rs, "top_n"),
            getInteger(rs, "stock_count"),
            getDouble(rs, "avg_total_score"),
            getDouble(rs, "max_total_score"),
            getDouble(rs, "min_total_score"),
            readListOfMaps(rs.getString("top_stocks")),
            rs.getString("object_key"),
            rs.getString("created_at")
        );
    }

    private ResultModels.SelectionSnapshotListItem mapSelectionSnapshotListItem(ResultSet rs, int rowNum) throws SQLException {
        return new ResultModels.SelectionSnapshotListItem(
            rs.getString("trade_date"),
            getInteger(rs, "top_n"),
            getInteger(rs, "stock_count"),
            getDouble(rs, "avg_total_score"),
            getDouble(rs, "max_total_score"),
            getDouble(rs, "min_total_score"),
            rs.getString("object_key"),
            toSelectionTopStocks(readListOfMaps(rs.getString("top_stocks"))),
            rs.getString("created_at")
        );
    }

    private ResultModels.BacktestSummaryResponse mapBacktestSummary(ResultSet rs, int rowNum) throws SQLException {
        return new ResultModels.BacktestSummaryResponse(
            rs.getString("run_key"),
            rs.getString("strategy_name"),
            rs.getString("start_date"),
            rs.getString("end_date"),
            rs.getString("rebalance_mode"),
            getDouble(rs, "initial_cash"),
            getDouble(rs, "commission_rate"),
            getDouble(rs, "slippage_bps"),
            getDouble(rs, "stamp_tax_rate"),
            getInteger(rs, "top_n"),
            rs.getString("execution_rule"),
            rs.getString("status"),
            readMap(rs.getString("metrics")),
            rs.getString("report_object_key"),
            rs.getString("detail_object_key"),
            rs.getString("created_at")
        );
    }

    private ResultModels.BacktestSummaryListItem mapBacktestSummaryListItem(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> metrics = readMap(rs.getString("metrics"));
        Map<String, Object> benchmarkReturns = readNestedMap(metrics, "benchmark_returns");
        return new ResultModels.BacktestSummaryListItem(
            rs.getString("run_key"),
            rs.getString("strategy_name"),
            rs.getString("start_date"),
            rs.getString("end_date"),
            rs.getString("rebalance_mode"),
            getInteger(rs, "top_n"),
            rs.getString("status"),
            getDouble(rs, "initial_cash"),
            getDouble(rs, "commission_rate"),
            getDouble(rs, "stamp_tax_rate"),
            getDouble(rs, "slippage_bps"),
            rs.getString("execution_rule"),
            numberValue(metrics, "total_return"),
            firstNumberValue(metrics, "annual_return", "annualized_return"),
            numberValue(metrics, "max_drawdown"),
            numberValue(benchmarkReturns, "000300.SH"),
            numberValue(benchmarkReturns, "000905.SH"),
            numberValue(benchmarkReturns, "000906.SH"),
            rs.getString("detail_object_key"),
            rs.getString("created_at")
        );
    }

    private ResultModels.UpdateLogEntryResponse mapUpdateLogEntry(ResultSet rs, int rowNum) throws SQLException {
        return new ResultModels.UpdateLogEntryResponse(
            rs.getString("trade_date"),
            rs.getString("step_name"),
            rs.getString("status"),
            rs.getString("object_key"),
            rs.getString("message"),
            rs.getString("updated_at")
        );
    }

    private ResultModels.TaskLogEntryResponse mapTaskLogEntry(ResultSet rs, int rowNum) throws SQLException {
        return new ResultModels.TaskLogEntryResponse(
            rs.getLong("id"),
            rs.getString("task_type"),
            rs.getString("status"),
            readMap(rs.getString("params")),
            readMap(rs.getString("result_summary")),
            rs.getString("object_key"),
            rs.getString("created_at"),
            rs.getString("updated_at")
        );
    }

    private Integer getInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private Double getDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private Map<String, Object> readMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException exc) {
            throw new DataRetrievalFailureException("invalid JSON object in database");
        }
    }

    private List<Map<String, Object>> readListOfMaps(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return objectMapper.readValue(json, LIST_OF_MAP_TYPE).stream()
                .map(LinkedHashMap::new)
                .map(map -> Map.<String, Object>copyOf(map))
                .toList();
        } catch (JsonProcessingException exc) {
            throw new DataRetrievalFailureException("invalid JSON array in database");
        }
    }

    private List<ResultModels.SelectionTopStock> toSelectionTopStocks(List<Map<String, Object>> rows) {
        return rows.stream()
            .map(row -> new ResultModels.SelectionTopStock(
                stringValue(row, "stockCode", "stock_code"),
                integerValue(row, "rank"),
                firstNumberValue(row, "totalScore", "total_score")
            ))
            .toList();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readNestedMap(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return result;
        }
        return Map.of();
    }

    private String stringValue(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Object value = map.get(key);
            if (value != null) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private Integer integerValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Integer.valueOf(text);
        }
        return null;
    }

    private Double firstNumberValue(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Double value = numberValue(map, key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private Double numberValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Double.valueOf(text);
        }
        return null;
    }
}
