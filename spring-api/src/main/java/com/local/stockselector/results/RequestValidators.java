package com.local.stockselector.results;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.regex.Pattern;

final class RequestValidators {
    private static final Pattern TRADE_DATE_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
    private static final Pattern RUN_KEY_PATTERN = Pattern.compile("^[a-f0-9]{16}$");
    private static final Pattern TASK_TYPE_PATTERN = Pattern.compile("^[A-Za-z0-9:_\\-.]{1,100}$");
    private static final Set<String> TASK_STATUSES = Set.of("pending", "running", "done", "failed");
    private static final Set<String> REBALANCE_MODES = Set.of("monthly", "quarterly");
    private static final int DEFAULT_PAGE_LIMIT = 20;
    private static final int DEFAULT_TASK_LOG_LIMIT = 20;

    private RequestValidators() {
    }

    static String tradeDate(String value) {
        if (value == null || !TRADE_DATE_PATTERN.matcher(value).matches()) {
            throw new BadRequestException("tradeDate must use YYYY-MM-DD");
        }
        try {
            LocalDate.parse(value);
            return value;
        } catch (DateTimeParseException exc) {
            throw new BadRequestException("tradeDate must be a valid date");
        }
    }

    static String runKey(String value) {
        if (value == null || !RUN_KEY_PATTERN.matcher(value).matches()) {
            throw new BadRequestException("runKey must be 16 lowercase hex characters");
        }
        return value;
    }

    static String taskStatus(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        if (!TASK_STATUSES.contains(value)) {
            throw new BadRequestException("status must be one of pending, running, done, failed");
        }
        return value;
    }

    static String rebalanceMode(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        if (!REBALANCE_MODES.contains(value)) {
            throw new BadRequestException("rebalanceMode must be monthly or quarterly");
        }
        return value;
    }

    static int pageLimit(Integer value) {
        if (value == null) {
            return DEFAULT_PAGE_LIMIT;
        }
        if (value < 1 || value > 100) {
            throw new BadRequestException("limit must be between 1 and 100");
        }
        return value;
    }

    static int pageOffset(Integer value) {
        if (value == null) {
            return 0;
        }
        if (value < 0) {
            throw new BadRequestException("offset must be greater than or equal to 0");
        }
        return value;
    }

    static String taskType(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        if (!TASK_TYPE_PATTERN.matcher(value).matches()) {
            throw new BadRequestException("taskType contains unsupported characters");
        }
        return value;
    }

    static int taskLogLimit(Integer value) {
        if (value == null) {
            return DEFAULT_TASK_LOG_LIMIT;
        }
        if (value < 1 || value > 100) {
            throw new BadRequestException("limit must be between 1 and 100");
        }
        return value;
    }
}
