package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.util.List;
import java.util.Map;

public class HivePartitionsSynchCli extends SimpleAbstractAppLauncher {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HivePartitionsSynchCli(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        Extractor extractor = new Extractor();
        Partitioner partitioner = new Partitioner(getConf());
        FileSystem fs = FileSystem.get(getConf());

        for (HivePartitionDTO dto: getHivePartitionDTOs()) {
            HivePartitionHolder holder = new HivePartitionHolder(dto);

            String wildcardPath = holder.getUserPattern();
            for (String partitionName: holder.getPartitionColumns()) {
                wildcardPath = wildcardPath.replace("{" + partitionName + "}", "*");
            }

            FileStatus[] statuses = fs.globStatus(new Path(holder.getParentPath() + wildcardPath));

            for (FileStatus status: statuses) {
                String path = status.getPath().toUri().getPath();
                if (!containsDisallowedPatterns(path) && status.isDirectory()) {
                    Map<String, String> partitionSpec = extractor.getPartitionSpec(holder, path);
                    if (partitionSpec != null) {
                        partitioner.create(holder.getTableName(), partitionSpec, path);
                    }
                }
            }
        }

        return ReturnCode.ALL_GOOD;
    }

    protected void initParser() {
        getParser().accepts(OPTION_CONFIG_FILE_FILE, "Local path to the configuration file.")
                .withRequiredArg().required();
    }

    protected boolean containsDisallowedPatterns(String path) {
        return path.contains("/tmp/")
                || path.contains("/_temporary/");
    }
}
