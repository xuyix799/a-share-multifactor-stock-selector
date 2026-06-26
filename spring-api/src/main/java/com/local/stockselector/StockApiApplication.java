package com.local.stockselector;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class StockApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(StockApiApplication.class, args);
    }
}

@RestController
class HealthController {
    private final DataSource dataSource;

    HealthController(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @GetMapping("/api/health")
    Map<String, String> health() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
        }
        return Map.of(
            "status", "UP",
            "database", "UP",
            "scope", "infrastructure"
        );
    }
}
