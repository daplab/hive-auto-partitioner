package ch.daplab.hivepartition;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Partitioner implements AutoCloseable {

    private static Logger LOG = LoggerFactory.getLogger(Partitioner.class);

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private final HiveConf hiveConf;
    private final String jdbcUri;
    private final Connection connection;

    public Partitioner(Configuration conf) throws Exception {

        hiveConf = new HiveConf();
        hiveConf.addResource(conf);
        URI uri = new URI(hiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREURIS));
        jdbcUri = "jdbc:hive2://" + uri.getHost() + ":" + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT) + "/default";

        Class.forName(driverName);
        connection = DriverManager.getConnection(jdbcUri, "", "");
    }

    public void create(String tableName, Map<String, String> partitionSpecs, String path) throws SQLException {

        try (Statement stmt = connection.createStatement()) {

            StringBuilder sb = new StringBuilder("ALTER TABLE ");

            sb.append("`");
            Joiner.on("`.`").appendTo(sb, tableName.split("\\."));
            sb.append("` ADD IF NOT EXISTS PARTITION (`");

            Joiner.on("',`").withKeyValueSeparator("`='").appendTo(sb, partitionSpecs);
            sb.append("') LOCATION '").append(path).append("'");

            LOG.warn("Generated query : {}", sb);

            stmt.execute(sb.toString());
        } catch (org.apache.hive.service.cli.HiveSQLException e) {
            LOG.warn("Got a HiveSQLException", e);
        }
    }

    public static void main (String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 5);
        Map<String, String> partitions = new HashMap();
        partitions.put("year", args[2]);
        partitions.put("month", args[3]);
        partitions.put("day", args[4]);

        try (Partitioner partitioner = new Partitioner(new Configuration())) {
            partitioner.create(args[1], partitions, "/tmp/1234/2345/3456");
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
