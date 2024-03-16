package whu.edu.cn.trajlab.example.query.advanced;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.BufferQueryConditon;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.advanced.BufferQuery;

import java.io.IOException;
import java.util.List;

import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

public class BufferQueryTest {
    @Test
    public void getTraRDDBufferQuery() throws IOException {
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        BufferQueryConditon bqc = new BufferQueryConditon(cenTrajectory, 0.2);
        BufferQuery bufferQuery = new BufferQuery(instance.getDataSet(DATASET_NAME), bqc);

        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            long start = System.currentTimeMillis();
            JavaRDD<Trajectory> rddQuery = bufferQuery.getRDDQuery(sparkSession);
            List<Trajectory> results = rddQuery.collect();
            System.out.println(results.size());
            for (Trajectory result : results) {
                System.out.println(result);
            }
            long end = System.currentTimeMillis();
            System.out.println("cost : " + (end - start) + "ms");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void getTraBufferQuery() throws IOException {
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        BufferQueryConditon bqc = new BufferQueryConditon(cenTrajectory, 0.2);
        BufferQuery bufferQuery = new BufferQuery(instance.getDataSet(DATASET_NAME), bqc);
        long start = System.currentTimeMillis();
        List<Trajectory> results = bufferQuery.executeQuery();
            System.out.println(results.size());
            for (Trajectory result : results) {
                System.out.println(result);
            }
            long end = System.currentTimeMillis();
            System.out.println("cost : " + (end - start) + "ms");
    }
}
