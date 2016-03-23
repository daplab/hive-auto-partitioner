package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.HivePartitionDTO;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public abstract class SimpleAbstractAppLauncher implements Tool {

    protected static final String OPTION_CONFIG_FILE_FILE = "configFile";
    protected static final String OPTION_HELP = "help";
    protected static final String OPTION_DRY_RUN = "dryrun";

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final OptionParser parser = new OptionParser();
    private Configuration conf;
    private OptionSet options;
    private File configFile;
    private List<HivePartitionDTO> hivePartitionDTOs;
    private boolean dryrun = false;
    private final ObjectMapper mapper = new ObjectMapper();

    protected final OptionSet getOptions() {
        return this.options;
    }
    protected final OptionParser getParser() { return this.parser; }
    public final Configuration getConf() {
        return this.conf;
    }
    public final void setConf(Configuration configuration) {
        this.conf = configuration;
    }
    protected ObjectMapper mapper() { return this.mapper; }
    protected File getConfigFile() { return this.configFile; }
    protected List<HivePartitionDTO> getHivePartitionDTOs() { return this.hivePartitionDTOs; }
    protected boolean isDryrun() { return dryrun; }

    @Override
    public final int run(String[] args) throws Exception {
        this.privateInitParser();
        boolean invalidOptions = false;

        try {
            this.options = this.getParser().parse(args);
        } catch (OptionException e) {
            invalidOptions = true;
            System.err.println("Invalid argument: " + e.getMessage());
            System.err.println("Run with --help for help.");
        }

        if(invalidOptions || this.options.has(OPTION_HELP)) {
            this.getParser().printHelpOn(System.out);
            return ReturnCode.HELP;
        } else {


            String configFileStr = (String) getOptions().valueOf(OPTION_CONFIG_FILE_FILE);
            dryrun = getOptions().has(OPTION_DRY_RUN);

            configFile = new File(configFileStr);

            if (!configFile.isFile() && !configFile.canRead()) {
                System.err.println("Configuration file " + configFileStr + " cannot be read. Please correct point to the right configuration file " +
                        "via --" + OPTION_CONFIG_FILE_FILE);
                return ReturnCode.GENERIC_WRONG_CONFIG;
            }

            hivePartitionDTOs = mapper().readValue(configFile, new TypeReference<List<HivePartitionDTO>>() {});

            return this.internalRun();
        }
    }

    protected abstract int internalRun() throws Exception;

    protected void initParser() {}

    protected void privateInitParser() {
        getParser().accepts(OPTION_CONFIG_FILE_FILE, "Local path to the configuration file.")
                .withRequiredArg().required();
        getParser().accepts(OPTION_DRY_RUN, "Dryrun, log but do not execute the DDL statements.");

        initParser();

        getParser().accepts("help", "Print this help").isForHelp();
    }

    protected class ReturnCode {
        public static final int ALL_GOOD = 0;
        public static final int HELP = 1;
        public static final int GENERIC_ERROR = 1;
        public static final int GENERIC_WRONG_CONFIG = 3;

        protected ReturnCode() {
        }
    }
}
