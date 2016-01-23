package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.util.List;
import java.util.Map;

public class HivePartitionsSynchCli extends AbstractAppLauncher {

    public static final String OPTION_CONFIG_FILE_FILE = "configFile";

    private final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HivePartitionsSynchCli(), args);
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

        Extractor extractor = new Extractor();
        Partitioner partitioner = new Partitioner(getConf());
        FileSystem fs = FileSystem.get(getConf());

        for (HivePartitionDTO dto: hivePartitionDTOs) {
            HivePartitionHolder holder = new HivePartitionHolder(dto);

            RemoteIterator<LocatedFileStatus> i = fs.listFiles(new Path(holder.getParentPath()), true);

            while (i.hasNext()) {
                LocatedFileStatus locatedFileStatus = i.next();
                String path = locatedFileStatus.getPath().toString();
                if (locatedFileStatus.isDirectory()) {
                    Map<String, String> partitionSpec = extractor.getPartitionSpec(holder, path);
                    if (partitionSpec != null) {
                        partitioner.create(holder.getTableName(), partitionSpec, path);
                    }
                }
            }
        }

        return 0;
    }

    protected void initParser() {
        getParser().accepts(OPTION_CONFIG_FILE_FILE, "Local path to the configuration file.")
                .withRequiredArg().required();
    }
}
