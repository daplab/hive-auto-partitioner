package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
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

        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource(conf);
        URI uri = new URI(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
        String jdbcUri = "jdbc:hive2://" + uri.getHost() + ":" + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT) + "/default";

        this.dataSource = HiveJDBCHelper.getDataSource(jdbcUri);
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
            } catch (org.apache.hive.service.cli.HiveSQLException e) {
                LOG.warn("Got a HiveSQLException", e);
                errorCount++;
                if (++retryCount == maxTries) throw new RuntimeException("Got a HiveSQLException " + retryCount + " times in a row, aborting.", e);
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
            } catch (org.apache.hive.service.cli.HiveSQLException e) {
                LOG.warn("Got a HiveSQLException", e);
                errorCount++;
                if (++retryCount == maxTries) throw new RuntimeException("Got a HiveSQLException " + retryCount + " times in a row, aborting.", e);
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
