package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.Helper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bperroud on 3/13/16.
 */
public class OtherTests {

    @Test
    public void testHelper() {

        String tableName = "default.table1";

        StringBuilder escapedTableName = Helper.escapeTableName(tableName);

        Assert.assertEquals("`default`.`table1`", escapedTableName.toString());

    }

    @Test
    public void testLocationParser() {

        String line = "Location:           \thdfs://daplab2/shared/zefix/sogc/2010/10/01";
        String location = line.replaceAll("Location:\\s*", "").trim();

        Assert.assertEquals("hdfs://daplab2/shared/zefix/sogc/2010/10/01", location);

    }

}
