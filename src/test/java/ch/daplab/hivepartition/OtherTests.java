package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created by bperroud on 3/13/16.
 */
public class OtherTests {

    @Test
    public void testHelper() {

        String tableName = "default.table1";

        StringBuilder escapedTableName = Helper.escapeTableName(tableName);

        Assert.assertEquals("`default`.`table1`", escapedTableName.toString());

        Map<String, String> partitionSpecs = new HashMap<>();
        partitionSpecs.put("year", "2016");
        partitionSpecs.put("month", "03");
        partitionSpecs.put("day", "13");

        String escapedPartitionSpecs = Helper.escapePartitionSpecs(partitionSpecs).toString();

        Assert.assertTrue(escapedPartitionSpecs.contains("`year`='2016'"));
        Assert.assertTrue(escapedPartitionSpecs.contains("`month`='03'"));
        Assert.assertTrue(escapedPartitionSpecs.contains("`day`='13'"));

    }

    @Test
    public void testLocationParser() {

        String line = "Location:           \thdfs://daplab2/shared/zefix/sogc/2010/10/01";
        String location = line.replaceAll("Location:\\s*", "").trim();

        Assert.assertEquals("hdfs://daplab2/shared/zefix/sogc/2010/10/01", location);
    }

    @Test
    public void testMatcher() throws Exception {

        List<String> exclusions = Collections.singletonList(".*/tmp/.*");

        Assert.assertTrue(Partitioner.containsDisallowedPatterns(exclusions, "/a/b/c/tmp/2016/03/03"));
        Assert.assertFalse(Partitioner.containsDisallowedPatterns(exclusions, "/a/b/c/2016/03/03"));

    }

}
