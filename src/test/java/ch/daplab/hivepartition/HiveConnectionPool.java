package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Created by bperroud on 3/23/16.
 */
public class HiveConnectionPool {

    private static Logger LOG = LoggerFactory.getLogger(Partitioner.class);

    protected static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        String jdbcUri = "jdbc:hive2://localhost:10000/default";

        Class.forName(driverName);

        BoneCPDataSource ds = new BoneCPDataSource();  // create a new datasource object
        ds.setConnectionTestStatement("SELECT 1");
        ds.setJdbcUrl(jdbcUri);		// set the JDBC url
        ds.setUsername("hdfs");				// set the username
        ds.setPassword("");				// set the password
        ds.setPartitionCount(1);
        ds.setAcquireIncrement(1);
        ds.setMaxConnectionsPerPartition(4);
        ds.setMinConnectionsPerPartition(1);
        ds.setStatementsCacheSize(0);
        ds.setIdleConnectionTestPeriod(60, TimeUnit.SECONDS);
        ds.setMaxConnectionAge(60, TimeUnit.MINUTES);
        ds.setIdleMaxAge(10, TimeUnit.MINUTES);

        try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {
            StringBuilder sb = new StringBuilder("SELECT 1");
            ResultSet rs = stmt.executeQuery(sb.toString());
            while (rs.next()) {
                String s = rs.getString(1);
                LOG.info(s);
            }
        } catch (org.apache.hive.service.cli.HiveSQLException e) {
            LOG.warn("Got a HiveSQLException", e);
        }
    }
}
