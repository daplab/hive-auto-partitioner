package ch.daplab.hivepartition.metrics;

import ch.daplab.hivepartition.rx.CreatePartitionObserver;
import ch.daplab.hivepartition.rx.DeletePartitionObserver;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import static com.yammer.metrics.core.MetricsRegistry.*;

/**
 * Created by bperroud on 3/21/16.
 */
public class MetricsHolder {

    static private final MetricsRegistry metrics = new MetricsRegistry();

    static private final Histogram createPartitionHistogram = metrics.newHistogram(CreatePartitionObserver.class, "time");
    static private final Counter createPartitionCounter = metrics.newCounter(CreatePartitionObserver.class, "count");

    static private final Histogram deletePartitionHistogram = metrics.newHistogram(DeletePartitionObserver.class, "time");
    static private final Counter deletePartitionCounter = metrics.newCounter(DeletePartitionObserver.class, "count");

    public static MetricsRegistry getMetrics() {
        return metrics;
    }

    public static Histogram getCreatePartitionHistogram() {
        return createPartitionHistogram;
    }

    public static Counter getCreatePartitionCounter() {
        return createPartitionCounter;
    }

    public static Histogram getDeletePartitionHistogram() {
        return deletePartitionHistogram;
    }

    public static Counter getDeletePartitionCounter() {
        return deletePartitionCounter;
    }
}
