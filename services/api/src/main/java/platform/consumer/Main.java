package platform.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Local experiment CLI. No HTTP interface and no arbitrary SQL. */
public final class Main {
    private Main() { }

    public static void main(String[] args) { System.exit(run(args, System.out)); }

    static int run(String[] args, PrintStream output) {
        try {
            if (args.length == 1 && args[0].equals("--help")) {
                output.println("UNVERIFIED JDBC POC: --gold-root PATH --publication SHA256 --report publishers|articles|daily|quality"
                        + " [--source ID] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--limit 1..1000]");
                return 0;
            }
            Set<String> allowed = Set.of("--gold-root", "--publication", "--report", "--source", "--start-date", "--end-date", "--limit");
            Map<String, String> options = new HashMap<>();
            if (args.length % 2 != 0) { throw new PocFailure("INVALID_ARGUMENT"); }
            for (int i = 0; i < args.length; i += 2) {
                if (!allowed.contains(args[i]) || options.put(args[i], args[i + 1]) != null) {
                    throw new PocFailure("INVALID_ARGUMENT");
                }
            }
            if (!options.keySet().containsAll(Set.of("--gold-root", "--publication", "--report"))) {
                throw new PocFailure("INVALID_ARGUMENT");
            }
            int limit = Integer.parseInt(options.getOrDefault("--limit", "100"));
            Object rows;
            try (JdbcReports reports = JdbcReports.open(Path.of(options.get("--gold-root")), options.get("--publication"))) {
                rows = reports.report(options.get("--report"), options.get("--source"),
                        options.get("--start-date"), options.get("--end-date"), limit);
            }
            output.println(new ObjectMapper().writeValueAsString(Map.of(
                    "status", "UNVERIFIED_POC", "publication", options.get("--publication"),
                    "report", options.get("--report"), "limit", limit, "rows", rows)));
            return 0;
        } catch (PocFailure e) {
            output.println("{\"status\":\"FAILED\",\"code\":\"" + e.getMessage() + "\"}");
            return "INVALID_ARGUMENT".equals(e.getMessage()) ? 2 : 1;
        } catch (IllegalArgumentException e) {
            output.println("{\"status\":\"FAILED\",\"code\":\"INVALID_ARGUMENT\"}");
            return 2;
        } catch (Exception | LinkageError e) {
            output.println("{\"status\":\"FAILED\",\"code\":\"POC_FAILED\"}");
            return 1;
        }
    }
}
