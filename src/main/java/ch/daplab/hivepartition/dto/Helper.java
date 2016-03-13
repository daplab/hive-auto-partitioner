package ch.daplab.hivepartition.dto;

import com.google.common.base.Joiner;

import java.util.Map;

/**
 * Created by bperroud on 3/13/16.
 */
public class Helper {
    
    public static StringBuilder escapeTableName(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("`");
        Joiner.on("`.`").appendTo(sb, tableName.split("\\."));
        sb.append("`");
        return sb;
    }

    public static StringBuilder escapePartitionSpecs(Map<String, String> partitionSpecs) {
        StringBuilder sb = new StringBuilder();
        sb.append("`");
        Joiner.on("',`").withKeyValueSeparator("`='").appendTo(sb, partitionSpecs);
        sb.append("'");
        return sb;
    }
}
