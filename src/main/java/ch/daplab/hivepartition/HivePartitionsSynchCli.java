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

    protected static final String OPTION_DROP_BEFORE_CREATE = "drop-before-create";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HivePartitionsSynchCli(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        boolean dropBeforeCreate = getOptions().has(OPTION_DROP_BEFORE_CREATE);

        Extractor extractor = new Extractor();
        Partitioner partitioner = new Partitioner(getConf(), isDryrun());
        FileSystem fs = FileSystem.get(getConf());

        for (HivePartitionDTO dto: getHivePartitionDTOs()) {
            long startTime = System.currentTimeMillis();
            int partitionCount = 0;

            HivePartitionHolder holder = new HivePartitionHolder(dto);

            String wildcardPath = holder.getUserPattern();
            for (String partitionName: holder.getPartitionColumns()) {
                wildcardPath = wildcardPath.replace("{" + partitionName + "}", "*");
            }

            FileStatus[] statuses = fs.globStatus(new Path(holder.getParentPath() + wildcardPath));

            for (FileStatus status: statuses) {
                String path = status.getPath().toUri().getPath();
                if (!partitioner.containsDisallowedPatterns(dto.getExclusions(), path) && status.isDirectory()) {
                    Map<String, String> partitionSpec = extractor.getPartitionSpec(holder, path);
                    if (partitionSpec != null) {
                        if (dropBeforeCreate) {
                            partitioner.delete(holder.getTableName(), partitionSpec);
                        }
                        partitionCount++;
                        partitioner.create(holder.getTableName(), partitionSpec, path);
                    }
                }
            }

            System.out.println("Processed table " + dto.getTableName() + " in " + (System.currentTimeMillis() - startTime) + "ms : " +
                    partitionCount + " partitions processed");


        }

        return ReturnCode.ALL_GOOD;
    }

    protected void initParser() {
        getParser().accepts(OPTION_DROP_BEFORE_CREATE,
                "Issue a DROP PARTITION statement before the CREATE PARTITION -- useful for migration to `hive.assume-canonical-partition-keys`");
    }

}
