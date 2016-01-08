package ch.daplab.hivepartition.dto;

import org.codehaus.jackson.annotate.JsonProperty;

public class HivePartitionDTO {

    @JsonProperty
    private String tableName;
    @JsonProperty
    private String parentPath;
    @JsonProperty
    private String pattern;

    public HivePartitionDTO() {}

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
