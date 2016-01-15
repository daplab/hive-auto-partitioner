package ch.daplab.hivepartition;

import com.google.common.base.Joiner;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by bperroud on 1/15/16.
 */
public class PartitionerTest {

    @Test
    public void test() {

        Map<String, String> partitionSpecs = new HashMap();
        partitionSpecs.put("year", "2016");
        partitionSpecs.put("month", "01");
        partitionSpecs.put("day", "14");

        StringBuilder sb = new StringBuilder();
        Joiner.on("',`").withKeyValueSeparator("`='").appendTo(sb, partitionSpecs);

        System.out.print(sb.toString());

    }
}