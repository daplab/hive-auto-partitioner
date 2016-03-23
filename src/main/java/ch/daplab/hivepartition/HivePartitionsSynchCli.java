package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class HivePartitionsSynchCli extends SimpleAbstractAppLauncher {

    protected static final String OPTION_DROP_BEFORE_CREATE = "drop-before-create";
    protected static final String OPTION_CHECK_BEFORE_CREATE = "check-before-create";
    protected static final String OPTION_DROP_WHEN_ERROR = "drop-when-error";
    protected static final String OPTION_CREATE_ONLY = "create-only";
    protected static final String OPTION_DROP_ONLY = "drop-only";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HivePartitionsSynchCli(), args);
        System.exit(res);
    }

    @Override
    public final int internalRun() throws Exception {

        boolean dropBeforeCreate = getOptions().has(OPTION_DROP_BEFORE_CREATE);
        boolean checkBeforeCreate = getOptions().has(OPTION_CHECK_BEFORE_CREATE);

        boolean dropWhenError = getOptions().has(OPTION_DROP_WHEN_ERROR);

        boolean createOnly = getOptions().has(OPTION_CREATE_ONLY);
        boolean dropOnly = getOptions().has(OPTION_DROP_ONLY);

        FileSystem fs = FileSystem.get(getConf());

        final DataSource dataSource = HiveJDBCHelper.getDataSource(HiveJDBCHelper.getJdbcUri(getConf()));

        Partitioner partitioner = new Partitioner(dataSource, isDryrun());

        Extractor extractor = new Extractor();

        for (HivePartitionDTO dto : getHivePartitionDTOs()) {

            LOG.info("Starting to process table {}", dto.getTableName());

            long startTime = System.currentTimeMillis();

            int partitionCount = 0;

            HivePartitionHolder holder = new HivePartitionHolder(dto);

            if ((dropOnly && !createOnly) || (!dropOnly && !createOnly)) {
                try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {
                    StringBuilder sb = new StringBuilder("show partitions ");
                    sb.append(Helper.escapeTableName(dto.getTableName()));
                    ResultSet rs = stmt.executeQuery(sb.toString());
                    partitionCount = 0;
                    int deletePartitionCount = 0;
                    while (rs.next()) {
                        String partition = rs.getString(1);
                        partitionCount++;

                        Map<String, String> partitionSpecs = new HashMap();
                        for (String spec : partition.split("/")) {
                            String[] parts = spec.split("=");
                            partitionSpecs.put(parts[0], parts[1]);
                        }

                        String location = getPartitionLocation(dto, partitionSpecs, connection);

                        if (location == null) {
                            if (dropWhenError) {
                                partitioner.delete(dto.getTableName(), partitionSpecs);
                            }
                        } else {

                            Path p = new Path(location);
                            boolean isDir = fs.isDirectory(p);
                            if (!p.toUri().getPath().startsWith(dto.getParentPath())) {
                                LOG.warn("Warning: location {} seems to be outside of the table parent folder {} for table {} ({})",
                                        p, dto.getParentPath(), dto.getTableName(), Helper.escapePartitionSpecs(partitionSpecs));
                            }

                            if (!isDir) {
                                partitioner.delete(dto.getTableName(), partitionSpecs);
                                deletePartitionCount++;
                            }
                        }
                    }
                    LOG.info("[DROP PARTITION] Processed table {} in {}ms : {}" +
                                    " partitions found, {} partitions deleted",
                            dto.getTableName(), (System.currentTimeMillis() - startTime), partitionCount, deletePartitionCount);

                } catch (org.apache.hive.service.cli.HiveSQLException e) {
                    LOG.warn("Got a HiveSQLException after " + partitionCount + " partitions", e);
                }
            }

            if ((createOnly && !dropOnly) || (!dropOnly && !createOnly)) {

                startTime = System.currentTimeMillis();
                partitionCount = 0;

                String wildcardPath = holder.getUserPattern();
                for (String partitionName : holder.getPartitionColumns()) {
                    wildcardPath = wildcardPath.replace("{" + partitionName + "}", "*");
                }

                FileStatus[] statuses = fs.globStatus(new Path(holder.getParentPath() + wildcardPath));

                Connection connection = dataSource.getConnection();

                for (FileStatus status : statuses) {
                    String path = status.getPath().toUri().getPath();
                    if (!partitioner.containsDisallowedPatterns(dto.getExclusions(), path) && status.isDirectory()) {
                        Map<String, String> partitionSpec = extractor.getPartitionSpec(holder, path);
                        if (partitionSpec != null) {
                            if (dropBeforeCreate) {
                                partitioner.delete(holder.getTableName(), partitionSpec);
                            }
                            if (checkBeforeCreate) {

                                String location = getPartitionLocation(dto, partitionSpec, connection);
                                if (location != null) {
                                    continue;
                                }
                            }
                            partitionCount++;
                            partitioner.create(holder.getTableName(), partitionSpec, path);
                        }
                    }
                }

                LOG.info("[ADD PARTITION] Processed table {} in {}ms : {} " +
                                "partitions processed",
                        dto.getTableName(), (System.currentTimeMillis() - startTime), partitionCount);
            }
        }
        return ReturnCode.ALL_GOOD;
    }

    protected void initParser() {
        getParser().accepts(OPTION_DROP_WHEN_ERROR,
                "Issue a DROP PARTITION statement when the partition is not found");
        getParser().accepts(OPTION_CHECK_BEFORE_CREATE,
                "Validate that the partition do not exists before executing a ADD PARTITION statement." +
                        " This will slow down the process");
        getParser().accepts(OPTION_DROP_WHEN_ERROR,
                "Issue a DROP PARTITION statement when the partition is not found");
        getParser().accepts(OPTION_CREATE_ONLY,
                "Run CREATE PARTITION only, i.e. skip the DROP PARTITION part.");
        getParser().accepts(OPTION_DROP_ONLY,
                "Run DROP PARTITION only, i.e. skip the CREATE PARTITION part.");

    }

    protected String getPartitionLocation(HivePartitionDTO dto, Map<String, String> partitionSpecs, Connection connection) {

        StringBuilder partitionSb = new StringBuilder("describe formatted ");
        partitionSb.append(Helper.escapeTableName(dto.getTableName()));
        partitionSb.append(" partition(");
        partitionSb.append(Helper.escapePartitionSpecs(partitionSpecs));
        partitionSb.append(")");

        LOG.debug("Partition query = {}", partitionSb);

        try (Statement partitionStmt = connection.createStatement()) {

            ResultSet partitionRs = partitionStmt.executeQuery(partitionSb.toString());

            while (partitionRs.next()) {
                String line = partitionRs.getString(1);

                if (line.startsWith("Location:")) {
                    return partitionRs.getString(2);
                }
            }
        } catch (SQLException e) {
            LOG.warn("Got an SQLException while querying the partition {} on table {}, query was {}, {}",
                    Helper.escapePartitionSpecs(partitionSpecs), dto.getTableName(), partitionSb.toString(), e.getMessage());

        }
        return null;
    }
}
