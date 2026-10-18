package platform.consumer;

import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** JDBC feasibility only. Successful results are not verified Gold publications. */
public final class JdbcReports implements AutoCloseable {
    private static final DateTimeFormatter UTC_MICROS = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
    private static final String SOURCE = " WHERE (CAST(? AS VARCHAR) IS NULL OR source_id = ?) ";
    private static final String DATES = " AND (CAST(? AS DATE) IS NULL OR ingestion_date >= CAST(? AS DATE))"
            + " AND (CAST(? AS DATE) IS NULL OR ingestion_date <= CAST(? AS DATE)) ";
    private static final Map<String, String> QUERIES = Map.of(
            "publishers", "SELECT source_id, count(*) AS article_count, sum(observation_count) AS observation_count,"
                    + " min(first_observed_at) AS first_observed_at, max(last_observed_at) AS last_observed_at,"
                    + " count(*) FILTER (WHERE published_at IS NULL) AS articles_without_published_at FROM data"
                    + SOURCE + " GROUP BY source_id ORDER BY source_id LIMIT ?",
            "articles", "SELECT * FROM data" + SOURCE + " ORDER BY last_observed_at DESC, article_id LIMIT ?",
            "daily", "SELECT source_id, ingestion_date, observation_count, distinct_article_count,"
                    + " distinct_ingestion_count, quality_warning_count, latest_ingested_at FROM data"
                    + SOURCE + DATES + " ORDER BY source_id, ingestion_date LIMIT ?",
            "quality", "SELECT source_id, sum(observation_count) AS observation_count,"
                    + " sum(quality_warning_count) AS quality_warning_count,"
                    + " sum(quality_warning_count)::DOUBLE / sum(observation_count) AS warning_codes_per_observation"
                    + " FROM data" + SOURCE + DATES + " GROUP BY source_id ORDER BY source_id LIMIT ?");
    private final PocPublication publication;
    private final Connection connection;

    private JdbcReports(PocPublication publication, Connection connection) {
        this.publication = publication;
        this.connection = connection;
    }

    public static JdbcReports open(Path root, String fingerprint) {
        PocPublication publication = PocPublication.select(root, fingerprint);
        Connection connection = null;
        try {
            Properties properties = new Properties();
            properties.setProperty("threads", "2");
            properties.setProperty("autoinstall_known_extensions", "false");
            properties.setProperty("autoload_known_extensions", "false");
            properties.setProperty("temp_directory", "");
            connection = DriverManager.getConnection("jdbc:duckdb:", properties);
            try (var statement = connection.createStatement()) {
                statement.execute("SET TimeZone = 'UTC'");
            }
            return new JdbcReports(publication, connection);
        } catch (SQLException | RuntimeException | LinkageError e) {
            if (connection != null) {
                try { connection.close(); } catch (SQLException ignored) { /* Preserve safe diagnostic. */ }
            }
            throw new PocFailure("JDBC_INITIALIZATION_FAILED");
        }
    }

    public List<Map<String, Object>> report(String report, String source, String start, String end, int limit) {
        boolean daily = "daily".equals(report) || "quality".equals(report);
        if (report == null || !QUERIES.containsKey(report) || limit < 1 || limit > 1000
                || (source != null && source.isBlank()) || (!daily && (start != null || end != null))) {
            throw new PocFailure("INVALID_ARGUMENT");
        }
        LocalDate first = date(start);
        LocalDate last = date(end);
        if (first != null && last != null && first.isAfter(last)) {
            throw new PocFailure("INVALID_ARGUMENT");
        }
        String table = daily ? "gold_source_daily" : "gold_articles";
        List<String> paths = publication.files().get(table);
        // Only placeholder count is constructed. All paths and filter values are parameters.
        String scan = "read_parquet([" + String.join(",", Collections.nCopies(paths.size(), "?"))
                + "], hive_partitioning=false, union_by_name=false)";
        String sql = "WITH data AS (SELECT * FROM " + scan + ") " + QUERIES.get(report);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            for (String path : paths) {
                statement.setString(index++, path);
            }
            statement.setString(index++, source);
            statement.setString(index++, source);
            if (daily) {
                statement.setObject(index++, first);
                statement.setObject(index++, first);
                statement.setObject(index++, last);
                statement.setObject(index++, last);
            }
            statement.setInt(index, limit);
            try (ResultSet result = statement.executeQuery()) {
                return rows(result);
            }
        } catch (SQLException | RuntimeException e) {
            throw new PocFailure("QUERY_FAILED");
        }
    }

    private static LocalDate date(String value) {
        if (value == null) { return null; }
        try {
            LocalDate parsed = LocalDate.parse(value);
            if (!parsed.toString().equals(value)) { throw new IllegalArgumentException(); }
            return parsed;
        } catch (RuntimeException e) {
            throw new PocFailure("INVALID_ARGUMENT");
        }
    }

    private static List<Map<String, Object>> rows(ResultSet result) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData columns = result.getMetaData();
        while (result.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columns.getColumnCount(); i++) {
                Object value = result.getObject(i);
                // Never use primitive getters for nullable columns or JVM-default timezone conversion.
                if (value != null) {
                    switch (columns.getColumnTypeName(i)) {
                        case "TIMESTAMP" -> value = result.getObject(i, LocalDateTime.class).format(UTC_MICROS);
                        case "DATE" -> value = result.getObject(i, LocalDate.class).toString();
                        default -> {
                            if (value instanceof Array array) {
                                try { value = new ArrayList<>(Arrays.asList((Object[]) array.getArray())); }
                                finally { array.free(); }
                            }
                        }
                    }
                }
                row.put(columns.getColumnLabel(i), value);
            }
            rows.add(row);
        }
        return rows;
    }

    boolean isClosed() throws SQLException { return connection.isClosed(); }

    @Override
    public void close() {
        try { connection.close(); }
        catch (SQLException e) { throw new PocFailure("JDBC_CLOSE_FAILED"); }
    }
}
