package whu.edu.cn.trajlab.example.query.advanced;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.AccompanyQueryCondition;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.advanced.AccompanyQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

public class AccompanyQueryTest {
    @Test
    public void getTrajAccQuery() throws IOException {
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        List<TrajPoint> pointList = cenTrajectory.getPointList();
        List<TrajPoint> trajPoints = new ArrayList<>();
        for(int i = 0; i < 20; i++){
            trajPoints.add(pointList.get(i));
        }
        Trajectory subTrajectory = new Trajectory(cenTrajectory.getTrajectoryID(), cenTrajectory.getObjectID(), trajPoints);
        AccompanyQueryCondition aqc = new AccompanyQueryCondition(subTrajectory, 1, 5, 5);
        AccompanyQuery accompanyQuery = new AccompanyQuery(instance.getDataSet(DATASET_NAME), aqc);
        long start = System.currentTimeMillis();
        List<Trajectory> results = accompanyQuery.executeQuery();
        System.out.println(results.size());
        for (Trajectory result : results) {
            System.out.println(result);
        }
        long end = System.currentTimeMillis();
        System.out.println("cost : " + (end - start) + "ms");
    }

    @Test
    public void getTrajAccRDDQuery() throws IOException {
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        List<TrajPoint> pointList = cenTrajectory.getPointList();
        List<TrajPoint> trajPoints = new ArrayList<>();
        for(int i = 0; i < 20; i++){
            trajPoints.add(pointList.get(i));
        }
        Trajectory subTrajectory = new Trajectory(cenTrajectory.getTrajectoryID(), cenTrajectory.getObjectID(), trajPoints);
        AccompanyQueryCondition aqc = new AccompanyQueryCondition(subTrajectory, 1, 5, 5);
        AccompanyQuery accompanyQuery = new AccompanyQuery(instance.getDataSet(DATASET_NAME), aqc);

        boolean isLocal = true;
        try (SparkSession sparkSession = SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            long start = System.currentTimeMillis();
            JavaRDD<Trajectory> rddQuery = accompanyQuery.getRDDQuery(sparkSession);
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
}
