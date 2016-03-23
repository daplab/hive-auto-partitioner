package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partitioner implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(Partitioner.class);

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private final HiveConf hiveConf;
    private final String jdbcUri;
    private final DataSource dataSource;
    private final boolean dryrun;

    private volatile int errorCount = 0;

    public Partitioner(HiveConf hiveConf, String jdbcUri, boolean dryrun, DataSource dataSource) throws Exception {
        this.hiveConf = hiveConf;
        this.jdbcUri = jdbcUri;
        this.dryrun = dryrun;
        this.dataSource = dataSource;
    }

    public Partitioner(Configuration conf, boolean dryrun) throws Exception {

        hiveConf = new HiveConf();
        hiveConf.addResource(conf);
        URI uri = new URI(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
        jdbcUri = "jdbc:hive2://" + uri.getHost() + ":" + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT) + "/default";

        Class.forName(driverName);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUri);
        config.setUsername("hdfs");
        config.setPassword("");
        config.addDataSourceProperty("cachePrepStmts", "false");

        this.dataSource = new HikariDataSource(config);

        this.dryrun = dryrun;
    }

    public void create(String tableName, Map<String, String> partitionSpecs, String path) throws SQLException {

        final Connection connection = dataSource.getConnection();

        try (Statement stmt = connection.createStatement()) {

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
        } catch (org.apache.hive.service.cli.HiveSQLException e) {
            LOG.warn("Got a HiveSQLException", e);
            errorCount++;
            ensureLowErrorCountOrShutdown();
        } finally {
            connection.close();
        }
    }

    public void delete(String tableName, Map<String, String> partitionSpecs) throws SQLException {

        final Connection connection = dataSource.getConnection();

        try (Statement stmt = connection.createStatement()) {

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
        } catch (org.apache.hive.service.cli.HiveSQLException e) {
            LOG.warn("Got a HiveSQLException", e);
            errorCount++;
            ensureLowErrorCountOrShutdown();
        } finally {
            connection.close();
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
        if (dataSource != null) {
            if (dataSource instanceof Closeable) {
                ((Closeable)dataSource).close();
            }
        }
    }

    private void ensureLowErrorCountOrShutdown() {
        if (errorCount > 10) {
            Runtime.getRuntime().halt(1);
        }
    }
}
