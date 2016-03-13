package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import com.google.common.base.Joiner;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HivePartitionsPurgeCli extends AbstractAppLauncher {

    public static final String OPTION_CONFIG_FILE_FILE = "configFile";

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HivePartitionsPurgeCli(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        String configFile = (String) getOptions().valueOf(OPTION_CONFIG_FILE_FILE);

        File f = new File(configFile);

        if (!f.isFile() && !f.canRead()) {
            System.err.println("Configuration file " + configFile + " cannot be read. Please correct point to the right configuration file " +
                    "via --" + OPTION_CONFIG_FILE_FILE);
            return ReturnCode.GENERIC_WRONG_CONFIG;
        }

        List<HivePartitionDTO> hivePartitionDTOs = mapper.readValue(f, new TypeReference<List<HivePartitionDTO>>() {});

        FileSystem fs = FileSystem.get(getConf());

        final HiveConf hiveConf;
        final String jdbcUri;
        final Connection connection;

        hiveConf = new HiveConf();
        hiveConf.addResource(getConf());
        URI uri = new URI(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
        jdbcUri = "jdbc:hive2://" + uri.getHost() + ":" + hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT) + "/default";

        Class.forName(driverName);
        connection = DriverManager.getConnection(jdbcUri, "hdfs", "");

        for (HivePartitionDTO dto: hivePartitionDTOs) {

            try (Statement stmt = connection.createStatement()) {
                StringBuilder sb = new StringBuilder("show partitions ");
                sb.append(Helper.escapeTableName(dto.getTableName()));
                ResultSet rs = stmt.executeQuery(sb.toString());
                int partitionCount = 0;
                int  deletePartitionCount = 0;
                while (rs.next()) {
                    String partition = rs.getString(1);

                    partition = partition.replace("/", "',").replace("=", "'=");
                    StringBuilder partitionSb = new StringBuilder("describe formatted ");
                    partitionSb.append(Helper.escapeTableName(dto.getTableName()));
                    partitionSb.append(" partition(");
                    partitionSb.append(partition);
                    partitionSb.append("')");

                    System.out.println("Partition query = " + partitionSb.toString());
                    ResultSet partitionRs = stmt.executeQuery(partitionSb.toString());

                    while (partitionRs.next()) {
                        String line = partitionRs.getString(1);

                        if (line.startsWith("Location:")) {
                            String location = line.split(":")[1].trim();

                            Path p = new Path(location);
                            boolean isDir = fs.isDirectory(p);
                            System.out.println(p + " is dir? " + isDir);

                            if (!isDir) {
                                StringBuilder alterSb = new StringBuilder("ALTER TABLE ");
                                alterSb.append(Helper.escapeTableName(dto.getTableName()));
                                alterSb.append(" DROP PARTITION(");
                                alterSb.append(partition);
                                alterSb.append("')");
                                System.out.println(alterSb.toString());
//                                stmt.execute(alterSb.toString());
                                deletePartitionCount++;
                            }
                            break;
                        }
                    }
                }

                System.out.println("" + partitionCount + " partitions found, " + deletePartitionCount + " partitions deleted");

            } catch (org.apache.hive.service.cli.HiveSQLException e) {
                LOG.warn("Got a HiveSQLException", e);
            }
        }

        return 0;
    }

    protected void initParser() {
        getParser().accepts(OPTION_CONFIG_FILE_FILE, "Local path to the configuration file.")
                .withRequiredArg().required();
    }
}