package ch.daplab.hivepartition.dto;

import com.google.common.base.Joiner;

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
}
