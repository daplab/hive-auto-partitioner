package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class Extractor {

    /**
     * Returns a Map of partition_column = partition_col_value, or null if the
     */
    public Map<String, String> getPartitionSpec(HivePartitionHolder holder, String path) {

        String partitionPath = path.substring(holder.getParentPath().length());

        Matcher matcher = holder.getPattern().matcher(partitionPath);

        if (!matcher.matches()) {
            return null;
        }

        Map<String, String> partitionSpec = new HashMap<>(holder.getPartitionColumns().size());

        for (String partitionColumn: holder.getPartitionColumns()) {
            String partitionColumnValue = matcher.group(partitionColumn);
            if (partitionColumnValue == null) {
                return null;
            }
            partitionSpec.put(partitionColumn, partitionColumnValue);
        }

        if (allFolders(partitionSpec)) {
            return partitionSpec;
        } else {
            return null;
        }
    }

    private boolean allFolders(Map<String, String> partitionSpec) {
        for (String value: partitionSpec.values()) {
            if (StringUtils.isEmpty(value) || value.contains("/")) {
                return false;
            }
        }
        return true;
    }
}
