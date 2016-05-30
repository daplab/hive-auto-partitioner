package ch.daplab.hivepartition.metrics;

import ch.daplab.hivepartition.HiveAutoPartitionerCli;
import ch.daplab.hivepartition.rx.CreatePartitionObserver;
import ch.daplab.hivepartition.rx.DeletePartitionObserver;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.*;


/**
 * Created by bperroud on 3/21/16.
 */
public class MetricsHolder {

    static private final MetricRegistry metrics = new MetricRegistry();

    static private final Histogram createPartitionHistogram = metrics.histogram(name(CreatePartitionObserver.class, "time"));
    static private final Counter createPartitionCounter = metrics.counter(name(CreatePartitionObserver.class, "count"));

    static private final Histogram deletePartitionHistogram = metrics.histogram(name(DeletePartitionObserver.class, "time"));
    static private final Counter deletePartitionCounter = metrics.counter(name(DeletePartitionObserver.class, "count"));
    static private final Meter exceptionMeter = metrics.meter(name(HiveAutoPartitionerCli.class, "exceptions"));

    public static MetricRegistry getMetrics() {
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

    public static Meter getExceptionMeter() {
        return exceptionMeter;
    }
}
