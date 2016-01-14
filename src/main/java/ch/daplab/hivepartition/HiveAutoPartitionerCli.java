package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import ch.daplab.hivepartition.rx.CreateDirectoryEventFilter;
import ch.daplab.hivepartition.rx.CreatePartitionObserver;
import com.verisign.utils.MultiPathTrie;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.client.InfiniteTrumpetEventStreamer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import rx.Observable;

import java.io.File;
import java.util.List;

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

        InfiniteTrumpetEventStreamer trumpetEventStreamer = new InfiniteTrumpetEventStreamer(getCuratorFrameworkKafka(),
                getTopic(), HiveAutoPartitionerCli.class.getCanonicalName() + "-" + getTopic());

        CreateDirectoryEventFilter filter = new CreateDirectoryEventFilter();
        CreatePartitionObserver observer = new CreatePartitionObserver(trie, getConf());

        Observable
                .from(trumpetEventStreamer)
                .filter(filter)
                .subscribe(observer);

        return 0;
    }

    protected void initParser() {
        getParser().accepts(OPTION_CONFIG_FILE_FILE, "Local path to the configuration file.")
                .withRequiredArg().required();
    }
}
