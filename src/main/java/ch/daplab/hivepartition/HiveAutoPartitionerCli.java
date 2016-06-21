package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import ch.daplab.hivepartition.metrics.MetricsHolder;
import ch.daplab.hivepartition.rx.CreatePartitionObserver;
import ch.daplab.hivepartition.rx.DeletePartitionObserver;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.verisign.utils.MultiPathTrie;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.client.InfiniteTrumpetEventStreamer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.observables.ConnectableObservable;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class HiveAutoPartitionerCli extends AbstractAppLauncher {

    public static final String OPTION_CONFIG_FILE_FILE = "configFile";

    private final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HiveAutoPartitionerCli(), args);
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

        MultiPathTrie<HivePartitionHolder> trie = new MultiPathTrie<>();
        for (HivePartitionDTO dto: hivePartitionDTOs) {
            HivePartitionHolder holder = new HivePartitionHolder(dto);
            trie.addOrAppendPath(holder.getParentPath(), holder);
        }

        final InfiniteTrumpetEventStreamer trumpetEventStreamer = new InfiniteTrumpetEventStreamer(getCuratorFrameworkKafka(),
                getTopic(), HiveAutoPartitionerCli.class.getCanonicalName() + "-" + getTopic());

        Partitioner partitioner = new Partitioner(getConf(), false);

        CreatePartitionObserver createPartitionObserver =
                new CreatePartitionObserver(trie, partitioner);
        DeletePartitionObserver deletePartitionObserver =
                new DeletePartitionObserver(trie, partitioner);

        final ConnectableObservable<Map<String, Object>> connectableObservable = Observable
                .from(trumpetEventStreamer).publish();

        final JmxReporter jmxReporter = JmxReporter.forRegistry(MetricsHolder.getMetrics()).build();
        jmxReporter.start();

        final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(MetricsHolder.getMetrics()).build();
        consoleReporter.start(5, TimeUnit.MINUTES);

        final AtomicReference<Subscription> createPartitionSubscriptionRef =
                new AtomicReference<>(null);
        final AtomicReference<Subscription> deletePartitionSubscriptionRef =
                new AtomicReference<>(null);

        Action1 onError = new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
                LOG.error("Got an irrecoverable exception, shutting down.", throwable);

                jmxReporter.stop();
                consoleReporter.stop();

                if (createPartitionSubscriptionRef.get() != null) {
                    createPartitionSubscriptionRef.get().unsubscribe();
                }

                if (deletePartitionSubscriptionRef.get() != null) {
                    deletePartitionSubscriptionRef.get().unsubscribe();
                }

                Runtime.getRuntime().exit(2);
            }
        };

        Action0 onCompleted = new Action0() {
            @Override
            public void call() {

            }
        };

        final Subscription createPartitionSubscription =
                connectableObservable.subscribe(createPartitionObserver, onError, onCompleted);
        createPartitionSubscriptionRef.set(createPartitionSubscription);
        final Subscription deletePartitionSubscription =
                connectableObservable.subscribe(deletePartitionObserver, onError, onCompleted);
        deletePartitionSubscriptionRef.set(deletePartitionSubscription);

        connectableObservable.connect();

        System.err.println("Let me know if you see this line :)");

        return 0;
    }

    protected void initParser() {
        getParser().accepts(OPTION_CONFIG_FILE_FILE, "Local path to the configuration file.")
                .withRequiredArg().required();
    }
}
