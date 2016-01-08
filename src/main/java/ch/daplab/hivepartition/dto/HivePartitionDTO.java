package ch.daplab.hivepartition.dto;

public class HivePartitionDTO {

    final String tableName;
    final String parentPath;
    final String pattern;

    public HivePartitionDTO(String tableName, String parentPath, String pattern) {
        this.tableName = tableName;
        this.parentPath = parentPath;
        this.pattern = pattern;
    }

    public String getTableName() {
        return tableName;
    }

    public String getParentPath() {
        return parentPath;
    }

    public String getPattern() {
        return pattern;
    }

}
