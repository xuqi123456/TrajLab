package whu.edu.cn.trajlab.example.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.BasicQuery;

import java.io.IOException;
import java.util.List;

import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.QUERY_WKT_INTERSECT;
import static whu.edu.cn.trajlab.query.query.QueryConf.*;

public class BasicQueryInterfaceTest extends Configured {
    @Test
    public void testScanQuery() throws IOException, ParseException {
        Configuration conf = new Configuration();
        conf.set(INDEX_TYPE, "XZ2");
        conf.set(DATASET_NAME, "TRAJECTORY_TEST");
        conf.set(SPATIAL_WINDOW, QUERY_WKT_INTERSECT);
        BasicQuery basicQuery = new BasicQuery(conf);
        List<Trajectory> scanQuery = basicQuery.getScanQuery();
        System.out.println(scanQuery.size());
    }
    @Test
    public void testRDDQuery() {
        Configuration conf = new Configuration();
        conf.set(INDEX_TYPE, "XZ2");
        conf.set(DATASET_NAME, "TRAJECTORY_TEST");
        conf.set(SPATIAL_WINDOW, QUERY_WKT_INTERSECT);
        BasicQuery basicQuery = new BasicQuery(conf);
        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddScanQuery = basicQuery.getRDDScanQuery(sparkSession);
            List<Trajectory> collect = rddScanQuery.collect();
            System.out.println(collect.size());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
