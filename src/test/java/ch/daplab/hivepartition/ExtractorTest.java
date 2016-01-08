package ch.daplab.hivepartition;

import ch.daplab.hivepartition.dto.HivePartitionDTO;
import ch.daplab.hivepartition.dto.HivePartitionHolder;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by bperroud on 1/8/16.
 */
public class ExtractorTest {


    @Test
    public void test() throws Exception {

        String tableName = "default.test";
        String parentPath = "/a/b/c/";

        Extractor extractor = new Extractor();

        String pattern1 = "{year}/{month}/{day}";
        String path1 = parentPath + "2016/01/07";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern1), path1));

        String pattern2 = "{year}/asdf/{month}/{day}";
        String path2 = parentPath + "2016/asdf/01/07";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern2), path2));

        String pattern3 = "t22-{year}/{month}/{day}/toto";
        String path3 = parentPath + "t22-2016/01/07/toto";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern3), path3));

        String pattern4 = "{year}-{month}-{day}";
        String path4 = parentPath + "2016-01-07";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern4), path4));

        String pattern5 = "u{user}/g{group}";
        String path5 = parentPath + "u1234/g345";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern5), path5));

        String pattern6 = "uasdfasdf";
        String path6 = parentPath + "234523452324352";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern6), path6));

        String pattern7 = "{asdf}/{qwer}";
        String path7 = parentPath + "12341241234";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern7), path7));

        String pattern8 = "abcd";
        String path8 = parentPath + "abcd";

        System.out.println(extractor.getPartitionInfo(init(tableName, parentPath, pattern8), path8));
    }

    public HivePartitionHolder init(String tableNAme, String parentPath, String pattern) {
        return new HivePartitionHolder(new HivePartitionDTO(tableNAme, parentPath, pattern));
    }
}