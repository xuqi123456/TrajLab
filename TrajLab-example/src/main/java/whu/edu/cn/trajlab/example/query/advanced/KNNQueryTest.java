package whu.edu.cn.trajlab.example.query.advanced;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.KNNQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.advanced.KNNQuery;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;
import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.*;

public class KNNQueryTest {
    public static TimeLine testTimeLine1;
    static List<TimeLine> timeLineList = new ArrayList<>();

    static {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
        ZonedDateTime start1 = ZonedDateTime.parse("2008-10-25 06:00:00", dateTimeFormatter);
        ZonedDateTime end1 = ZonedDateTime.parse("2008-12-04 11:00:00", dateTimeFormatter);
        testTimeLine1 = new TimeLine(start1, end1);
        timeLineList.add(testTimeLine1);
    }

    @Test
    public void getPointKNNQuery() throws IOException {
        long start = System.currentTimeMillis();
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        TrajPoint trajPoint = loadHBase.get(22).getPointList().get(0);
        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(18, trajPoint);
        KNNQuery knnQuery = new KNNQuery(instance.getDataSet(DATASET_NAME), knnQueryCondition);

        List<Trajectory> results = knnQuery.executeQuery();
        System.out.println(results.size());
        for (Trajectory result : results) {
            System.out.println(result);
        }
        long end = System.currentTimeMillis();
        System.out.println("cost : " + (end - start) + "ms");
    }

    @Test
    public void getPointKNNRDDQuery() throws IOException {
        long start = System.currentTimeMillis();
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        TrajPoint trajPoint = loadHBase.get(22).getPointList().get(0);
        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(18, trajPoint);
        KNNQuery knnQuery = new KNNQuery(instance.getDataSet(DATASET_NAME), knnQueryCondition);

        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = knnQuery.getRDDQuery(sparkSession);
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
    public void getPointKNNRDDQueryWithTime() throws IOException {
        long start = System.currentTimeMillis();
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        TrajPoint trajPoint = loadHBase.get(22).getPointList().get(0);
        TemporalQueryCondition tqc = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(7, trajPoint, tqc);
        KNNQuery knnQuery = new KNNQuery(instance.getDataSet(DATASET_NAME), knnQueryCondition);

        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = knnQuery.getRDDQuery(sparkSession);
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
    public void getTrajKNNRDDQuery() throws IOException {
        long start = System.currentTimeMillis();
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);

        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(18, cenTrajectory);
        KNNQuery knnQuery = new KNNQuery(instance.getDataSet(DATASET_NAME), knnQueryCondition);

        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = knnQuery.getRDDQuery(sparkSession);
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
    public void getTrajKNNRDDQueryWithTime() throws IOException {
        long start = System.currentTimeMillis();
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        TemporalQueryCondition tqc = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(7, cenTrajectory, tqc);
        KNNQuery knnQuery = new KNNQuery(instance.getDataSet(DATASET_NAME), knnQueryCondition);

        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = knnQuery.getRDDQuery(sparkSession);
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
