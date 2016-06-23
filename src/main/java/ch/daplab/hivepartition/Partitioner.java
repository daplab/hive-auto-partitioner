package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import ch.daplab.hivepartition.metrics.MetricsHolder;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partitioner implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(Partitioner.class);

    private final DataSource dataSource;
    private final boolean dryrun;

    private volatile int errorCount = 0;
    private static final int maxTries = 3;
    private final boolean internalDS;

    public Partitioner(DataSource dataSource, boolean dryrun) throws Exception {
        this.dataSource = dataSource;
        this.dryrun = dryrun;
        internalDS = false;
    }

    public Partitioner(Configuration conf, boolean dryrun) throws Exception {
        this.dataSource = HiveJDBCHelper.getDataSource(conf);
        internalDS = true;
        this.dryrun = dryrun;
    }

    public void create(String tableName, Map<String, String> partitionSpecs, String path) throws SQLException {

        int retryCount = 0;
        while(true) {
            try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {

                StringBuilder sb = new StringBuilder("ALTER TABLE ");

                sb.append(Helper.escapeTableName(tableName));
                sb.append(" ADD IF NOT EXISTS PARTITION (");
                sb.append(Helper.escapePartitionSpecs(partitionSpecs));
                sb.append(") LOCATION '").append(path).append("'");

                LOG.info("Generated query : {}", sb);

                if (!dryrun) {
                    stmt.execute(sb.toString());
                    errorCount = 0;
                }
                break;
            } catch (org.apache.hive.service.cli.HiveSQLException e) {
                LOG.warn("Got a HiveSQLException", e);
                MetricsHolder.getExceptionMeter().mark();
                if (e.getMessage().contains("SemanticException [Error 10248]")) {
                    // ignore this exception, most likely a string to int conversion failure.
                } else if (e.getMessage().contains("SemanticException [Error 10001]: Table not found ")) {
                    // ignore this exception, most likely an event on a table not created yet.
                } else if (e.getMessage().contains("SemanticException Unexpected unknown partitions ")) {
                    // ignore this exception, most likely temp folders we're not able to parse
                } else {
                    errorCount++;
                    if (++retryCount >= maxTries) {
                        LOG.error("Got {} exception in a raw, throwing it further",maxTries, e);
                        throw new RuntimeException("Got a HiveSQLException " + retryCount + " times in a row, aborting.", e);
                    }
                }
            }
        }
    }

    public void delete(String tableName, Map<String, String> partitionSpecs) throws SQLException {

        int retryCount = 0;
        while(true) {
            try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {

                StringBuilder sb = new StringBuilder("ALTER TABLE ");

                sb.append(Helper.escapeTableName(tableName));
                sb.append(" DROP IF EXISTS PARTITION (");
                sb.append(Helper.escapePartitionSpecs(partitionSpecs));
                sb.append(")");

                LOG.info("Generated query : {}", sb);

                if (!dryrun) {
                    stmt.execute(sb.toString());
                    errorCount = 0;
                }
                break;
            } catch (org.apache.hive.service.cli.HiveSQLException e) {
                LOG.warn("Got a HiveSQLException", e);
                MetricsHolder.getExceptionMeter().mark();
                if (e.getMessage().contains("SemanticException [Error 10248]")) {
                    // ignore this exception, most likely a string to int conversion failure.
                } else if (e.getMessage().contains("SemanticException [Error 10001]: Table not found ")) {
                    // ignore this exception, most likely an event on a table not created yet.
                } else if (e.getMessage().contains("SemanticException Unexpected unknown partitions ")) {
                    // ignore this exception, most likely temp folders we're not able to parse
                } else {
                    errorCount++;
                    if (++retryCount == maxTries) {
                        LOG.error("Got {} exception in a raw, throwing it further",maxTries, e);
                        throw new RuntimeException("Got a HiveSQLException " + retryCount + " times in a row, aborting.", e);
                    }
                }
            }
        }
    }

    public static boolean containsDisallowedPatterns(List<String> exclusions, String path) {
        for (String exclusion: exclusions) {
            boolean matches = path.matches(exclusion);
            if (matches) {
                return true;
            }
        }
        return false;
    }

    public static void main (String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 5);
        Map<String, String> partitions = new HashMap();
        partitions.put("year", args[2]);
        partitions.put("month", args[3]);
        partitions.put("day", args[4]);

        try (Partitioner partitioner = new Partitioner(new Configuration(), true)) {
            partitioner.create(args[1], partitions, "/tmp/1234/2345/3456");
        }
    }

    @Override
    public void close() throws IOException {
        if (internalDS && dataSource != null) {
            if (dataSource instanceof Closeable) {
                ((Closeable)dataSource).close();
            }
        }
    }
}
