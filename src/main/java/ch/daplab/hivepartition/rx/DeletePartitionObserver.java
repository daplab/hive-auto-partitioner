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

public class DeletePartitionObserver implements Action1<Map<String, Object>> {

    private static Logger LOG = LoggerFactory.getLogger(DeletePartitionObserver.class);

    private final MultiPathTrie<HivePartitionHolder> hivePartitionsTrie;

    private final Extractor extractor = new Extractor();
    private final Partitioner partitioner;

    public DeletePartitionObserver(MultiPathTrie<HivePartitionHolder> hivePartitionsTrie, Partitioner partitioner) {
        this.hivePartitionsTrie = hivePartitionsTrie;
        this.partitioner = partitioner;
    }

    @Override
    public void call(Map<String, Object> event) {

        String eventType = (String) event.get(EventAndTxId.FIELD_EVENTTYPE);
        String path;

        // The logic is the following:
        // An UNLINK event is generated if -skipTrash is provided, which is not that frequent.
        // If the deleted file/folder goes into Trash, it's a RENAME event fired up.
        //
        // The idea is to catch the rename from a managed folder and remove the partition, if any
        // A corner case is to upload into a tmp folder inside the managed folder and
        // moved to the right partition. CreatePartitionObserver should catch this event too
        // so it could be ignored further here.

        switch (eventType) {
            case "UNLINK":
                path = (String) event.get(EventAndTxId.FIELD_PATH);
                break;
            case "RENAME":
                path = (String) event.get(EventAndTxId.FIELD_SRCPATH);
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

                if (partitionSpec != null) {

                    LOG.info("Deleting partition based on event {} and partition {}, with partition spec {}", event, holder, partitionSpec);

                    MetricsHolder.getDeletePartitionCounter().inc();
                    final long startTime = System.currentTimeMillis();
                    try {
                        partitioner.delete(holder.getTableName(), partitionSpec);
                    } catch (SQLException e) {
                        LOG.warn("Exception while deleting the partition on table {}, with partition spec {} on event {}", holder, partitionSpec, event, e);

                        if (e.getCause() != null
                                && e.getCause() instanceof org.apache.thrift.transport.TTransportException) {
                            LOG.error("Don't know how to recover from this one, dying.");
                            Runtime.getRuntime().halt(1);
                        }
                    } finally {
                        MetricsHolder.getDeletePartitionHistogram().update(System.currentTimeMillis() - startTime);
                    }
                }
            }
        }
    }

}
