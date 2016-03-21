package ch.daplab.hivepartition.dto;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bperroud on 1/8/16.
 */
public class HivePartitionHolder {

    private static final Pattern PARTITION_COLUMN_EXTRACTOR = Pattern.compile("\\{[^\\}]*\\}");

    final HivePartitionDTO dto;
    final Pattern pattern;
    final Set<String> partitionColumns;

    public HivePartitionHolder(HivePartitionDTO dto) {
        this.dto = dto;

        Matcher matcher1 = PARTITION_COLUMN_EXTRACTOR.matcher(dto.getPattern());

        partitionColumns = new HashSet<>();
        String tmpPattern = dto.getPattern();
        while (matcher1.find()) {
            String group = matcher1.group(0);
            String partitionColumn = group.substring(1, group.length() - 1);
            partitionColumns.add(partitionColumn);
            tmpPattern = tmpPattern.replace(group, "(?<" + partitionColumn + ">.*?)");
            // Note: .*? means non greedy matching.
        }

        pattern = Pattern.compile(tmpPattern);
    }

    public String getTableName() {
        return dto.getTableName();
    }

    public String getParentPath() {
        return dto.getParentPath();
    }

    public String getUserPattern() {
        return dto.getPattern();
    }

    public List<String> getExclusions() {
        return dto.getExclusions();
    }

    public Pattern getPattern() {
        return pattern;
    }

    public Set<String> getPartitionColumns() {
        return partitionColumns;
    }
}
