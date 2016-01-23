package ch.daplab.hivepartition.rx;

import ch.daplab.hivepartition.Extractor;
import ch.daplab.hivepartition.Partitioner;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
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
        String path = (String) event.get(EventAndTxId.FIELD_PATH);

        if (!("UNLINK".equals(eventType))) {
            return;
        }

        LOG.trace("Processing {}", event);

        final Collection<Map.Entry<String, Collection<HivePartitionHolder>>> allMatchingDefinitions =
                hivePartitionsTrie.findAllMatchingPrefix(path);

        for (Map.Entry<String, Collection<HivePartitionHolder>> entries : allMatchingDefinitions) {

            for (HivePartitionHolder holder : entries.getValue()) {

                Map<String, String> partitionSpec = extractor.getPartitionSpec(holder, path);

                if (partitionSpec != null) {

                    LOG.debug("Deleting partition based on event {} and partition {}, with partition spec {}", event, holder, partitionSpec);

                    try {
                        partitioner.delete(holder.getTableName(), partitionSpec);
                    } catch (SQLException e) {
                        LOG.warn("Exception while deleting the partition {}, with partition spec {} on event {}", holder, partitionSpec, event, e);
                    }
                }
            }
        }
    }

}
