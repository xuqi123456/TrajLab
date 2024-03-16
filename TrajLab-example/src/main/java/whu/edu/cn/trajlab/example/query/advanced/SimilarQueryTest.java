package whu.edu.cn.trajlab.example.query.advanced;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.SimilarQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.advanced.SimilarQuery;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;
import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

public class SimilarQueryTest {
    public static TimeLine testTimeLine1;
    static List<TimeLine> timeLineList = new ArrayList<>();

    static {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
        ZonedDateTime start1 = ZonedDateTime.parse("2008-10-20 06:00:00", dateTimeFormatter);
        ZonedDateTime end1 = ZonedDateTime.parse("2008-11-05 11:00:00", dateTimeFormatter);
        testTimeLine1 = new TimeLine(start1, end1);
        timeLineList.add(testTimeLine1);
    }

    @Test
    public void getTrajSimQuery() throws IOException {
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        SimilarQueryCondition sqc = new SimilarQueryCondition(cenTrajectory, 10);
        SimilarQuery similarQuery = new SimilarQuery(instance.getDataSet(DATASET_NAME), sqc);
        long start = System.currentTimeMillis();
        List<Trajectory> results = similarQuery.executeQuery();
        System.out.println(results.size());
        for (Trajectory result : results) {
            System.out.println(result);
        }
        long end = System.currentTimeMillis();
        System.out.println("cost : " + (end - start) + "ms");
    }

    @Test
    public void getTrajSimRDDQuery() throws IOException {
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        SimilarQueryCondition sqc = new SimilarQueryCondition(cenTrajectory, 10);
        SimilarQuery similarQuery = new SimilarQuery(instance.getDataSet(DATASET_NAME), sqc);

        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            long start = System.currentTimeMillis();
            JavaRDD<Trajectory> rddQuery = similarQuery.getRDDQuery(sparkSession);
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
    public void getTrajSimRDDQueryWithTime() throws IOException {
        Database instance = Database.getInstance();
        List<Trajectory> loadHBase = getLoadHBase();
        Trajectory cenTrajectory = loadHBase.get(22);
        TemporalQueryCondition tqc = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);

        SimilarQueryCondition sqc = new SimilarQueryCondition(cenTrajectory, 10, tqc);
        SimilarQuery similarQuery = new SimilarQuery(instance.getDataSet(DATASET_NAME), sqc);

        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            long start = System.currentTimeMillis();
            JavaRDD<Trajectory> rddQuery = similarQuery.getRDDQuery(sparkSession);
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
