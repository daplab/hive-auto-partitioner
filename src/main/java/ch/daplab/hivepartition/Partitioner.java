package ch.daplab.hivepartition;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hkuppuswamy on 1/7/16.
 */
public class Partitioner {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public void create(String jdbcURL, String tableName, Map<String, String> partitions) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection(jdbcURL, "", "");
        Statement stmt = con.createStatement();

        StringBuilder builder = new StringBuilder("ALTER TABLE "+tableName+" ADD PARTITION (");

        builder.append(Joiner.on(",").withKeyValueSeparator("=").join(partitions));
        builder.append(")");

        System.out.println("Generated query : "+builder);
        stmt.execute(builder.toString());
        con.close();
    }

    public static void main (String[] args) throws SQLException {
        Preconditions.checkArgument(args.length == 5);
        Partitioner partitioner = new Partitioner();
        Map<String, String> partitions = new HashMap();
        partitions.put("year", args[2]);
        partitions.put("month", args[3]);
        partitions.put("day", args[4]);

        partitioner.create(args[0], args[1], partitions);
    }

}
