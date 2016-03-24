package ch.daplab.hivepartition;

import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * Created by bperroud on 3/23/16.
 */
public class HiveJDBCHelper {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static DataSource getDataSource(Configuration conf) throws ClassNotFoundException, URISyntaxException {
        String jdbcUri = getJdbcUri(conf);
        return getDataSource(jdbcUri);
    }

    public static DataSource getDataSource(String jdbcUri) throws ClassNotFoundException {

        Class.forName(driverName);

        BoneCPDataSource ds = new BoneCPDataSource();
        ds.setConnectionTestStatement("SELECT 1");
        ds.setJdbcUrl(jdbcUri);
        ds.setUsername("hdfs");
        ds.setPassword("");
        ds.setPartitionCount(1);
        ds.setAcquireIncrement(1);
        ds.setMaxConnectionsPerPartition(4);
        ds.setMinConnectionsPerPartition(1);
        ds.setStatementsCacheSize(0);
        ds.setIdleConnectionTestPeriod(60, TimeUnit.SECONDS);
        ds.setMaxConnectionAge(60, TimeUnit.MINUTES);
        ds.setIdleMaxAge(10, TimeUnit.MINUTES);

        return ds;
    }

    public static String getJdbcUri(Configuration conf) throws URISyntaxException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource(conf);
        URI uri = new URI(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
        String jdbcUri = "jdbc:hive2://" + uri.getHost() + ":" + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT) + "/default";
        return jdbcUri;
    }
}
