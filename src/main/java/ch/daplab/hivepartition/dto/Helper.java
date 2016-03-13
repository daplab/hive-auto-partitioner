package ch.daplab.hivepartition.dto;

/**
 * Created by bperroud on 3/13/16.
 */
public class Helper {
    
    public static StringBuilder escapeTableName(String tableName) {
        StringBuilder sb = new StringBuilder();
        final String[] splits = tableName.split("\\.");
        for (String split: splits) {
            sb.append('`');
            sb.append(split);
            sb.append('`');
        }
        return sb;
    }
}
