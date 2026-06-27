package com.local.stockselector.results;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(BadRequestException.class)
    ResponseEntity<ResultModels.ApiError> handleBadRequest(BadRequestException exc, HttpServletRequest request) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", exc.getMessage(), request);
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    ResponseEntity<ResultModels.ApiError> handleNotFound(ResourceNotFoundException exc, HttpServletRequest request) {
        return error(HttpStatus.NOT_FOUND, "NOT_FOUND", exc.getMessage(), request);
    }

    @ExceptionHandler({
        MethodArgumentTypeMismatchException.class,
        MissingServletRequestParameterException.class
    })
    ResponseEntity<ResultModels.ApiError> handleInvalidServletRequest(Exception exc, HttpServletRequest request) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "invalid request parameter", request);
    }

    @ExceptionHandler(DataAccessException.class)
    ResponseEntity<ResultModels.ApiError> handleDatabaseError(DataAccessException exc, HttpServletRequest request) {
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error", request);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ResultModels.ApiError> handleUnexpectedError(Exception exc, HttpServletRequest request) {
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Internal server error", request);
    }

    private ResponseEntity<ResultModels.ApiError> error(HttpStatus status, String code, String message, HttpServletRequest request) {
        return ResponseEntity.status(status).body(new ResultModels.ApiError(code, message, request.getRequestURI()));
    }
}
