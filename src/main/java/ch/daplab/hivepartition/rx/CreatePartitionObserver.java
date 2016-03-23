package ch.daplab.hivepartition.rx;

import ch.daplab.hivepartition.Extractor;
import ch.daplab.hivepartition.Partitioner;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import ch.daplab.hivepartition.metrics.MetricsHolder;
import com.verisign.utils.MultiPathTrie;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class CreatePartitionObserver implements Action1<Map<String, Object>> {

    private static Logger LOG = LoggerFactory.getLogger(CreatePartitionObserver.class);

    private final MultiPathTrie<HivePartitionHolder> hivePartitionsTrie;

    private final Extractor extractor = new Extractor();
    private final Partitioner partitioner;

    public CreatePartitionObserver(MultiPathTrie<HivePartitionHolder> hivePartitionsTrie, Partitioner partitioner) {
        this.hivePartitionsTrie = hivePartitionsTrie;
        this.partitioner = partitioner;
    }

    @Override
    public void call(Map<String, Object> event) {

        String eventType = (String) event.get(EventAndTxId.FIELD_EVENTTYPE);
        String path;

        switch (eventType) {
            case "CREATE":
                path = (String) event.get(EventAndTxId.FIELD_PATH);
                String nodeType = (String) event.get("iNodeType");
                if (!"DIRECTORY".equals(nodeType)) {
                    return;
                }
                break;
            case "RENAME":
                path = (String) event.get(EventAndTxId.FIELD_DSTPATH);
                break;
            default:
                return;
        }


        LOG.trace("Processing {}", event);

        final Collection<Map.Entry<String, Collection<HivePartitionHolder>>> allMatchingDefinitions =
                hivePartitionsTrie.findAllMatchingPrefix(path);

        for (Map.Entry<String, Collection<HivePartitionHolder>> entries : allMatchingDefinitions) {

            for (HivePartitionHolder holder : entries.getValue()) {

                Map<String, String> partitionSpec = extractor.getPartitionSpec(holder, path);

                if (partitionSpec != null && !partitioner.containsDisallowedPatterns(holder.getExclusions(), path)) {

                    LOG.info("Creating partition based on event {} and partition {}, with partition spec {}", event, holder, partitionSpec);

                    MetricsHolder.getCreatePartitionCounter().inc();
                    final long startTime = System.currentTimeMillis();
                    try {
                        partitioner.create(holder.getTableName(), partitionSpec, path);
                    } catch (SQLException e) {
                        LOG.warn("Exception while creating the partition on table {}, with partition spec {} on event {}", holder.getTableName(), partitionSpec, event, e);

                        if (e.getCause() != null
                                && e.getCause() instanceof org.apache.thrift.transport.TTransportException) {
                            LOG.error("Don't know how to recover from this one, dying.");
                            throw new RuntimeException("Please shutdown.", e);
                        }
                    } finally {
                        MetricsHolder.getCreatePartitionHistogram().update(System.currentTimeMillis() - startTime);
                    }
                }
            }
        }
    }
}
