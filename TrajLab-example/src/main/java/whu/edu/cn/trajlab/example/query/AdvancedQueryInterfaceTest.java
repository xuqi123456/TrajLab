package whu.edu.cn.trajlab.example.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.AdvancedQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.query.query.QueryConf.*;

public class AdvancedQueryInterfaceTest {
    @Test
    public void testScanQuery() throws IOException {
        List<Trajectory> loadHBase = getLoadHBase();
        TrajPoint trajPoint = loadHBase.get(22).getPointList().get(0);
        byte[] byteArray = SerializerUtils.serializeObject(trajPoint);
        // 将字节数组转换为Base64编码的字符串
        String trajPoint_str = Base64.getEncoder().encodeToString(byteArray);
        Configuration conf = new Configuration();
        conf.set(INDEX_TYPE, "KNN");
        conf.set(DATASET_NAME, "TRAJECTORY_TEST");
        conf.set(K, "7");
        conf.set(CENTER_POINT, trajPoint_str);
//        conf.set(START_TIME, "2008-10-25 06:00:00");
//        conf.set(END_TIME, "2008-12-04 11:00:00");
        AdvancedQuery advancedQuery = new AdvancedQuery(conf);
        List<Trajectory> scanQuery = advancedQuery.getScanQuery();
        System.out.println(scanQuery.size());
    }
    @Test
    public void testRDDQuery() throws IOException {
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        List<TrajPoint> pointList = cenTrajectory.getPointList();
        List<TrajPoint> trajPoints = new ArrayList<>();
        for(int i = 0; i < 20; i++){
            trajPoints.add(pointList.get(i));
        }
        Trajectory subTrajectory = new Trajectory(cenTrajectory.getTrajectoryID(), cenTrajectory.getObjectID(), trajPoints);

        byte[] byteArray = SerializerUtils.serializeObject(subTrajectory);
        // 将字节数组转换为Base64编码的字符串
        String trajectory_str = Base64.getEncoder().encodeToString(byteArray);
        Configuration conf = new Configuration();
        conf.set(INDEX_TYPE, "ACCOMPANY");
        conf.set(DATASET_NAME, "TRAJECTORY_TEST");
        conf.set(CENTER_TRAJECTORY, trajectory_str);
        conf.set(DIS_THRESHOLD, "1");
        conf.set(TIME_THRESHOLD, "5");
        conf.set(TIME_PERIOD, "DAY");
        conf.set(K, "5");
        AdvancedQuery advancedQuery = new AdvancedQuery(conf);
        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddScanQuery = advancedQuery.getRDDScanQuery(sparkSession);
            List<Trajectory> collect = rddScanQuery.collect();
            System.out.println(collect.size());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
