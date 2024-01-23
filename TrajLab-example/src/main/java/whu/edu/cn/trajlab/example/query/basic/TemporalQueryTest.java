package whu.edu.cn.trajlab.example.query.basic;

import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.basic.TemporalQuery;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;
import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

/**
 * @author xuqi
 * @date 2024/01/24
 */
public class TemporalQueryTest extends TestCase {
    public static TemporalQueryCondition temporalContainCondition;
    public static TemporalQueryCondition temporalIntersectCondition;
    public static TimeLine testTimeLine1;
    public static TimeLine testTimeLine2;

    static List<TimeLine> timeLineList = new ArrayList<>();
    static {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
        ZonedDateTime start1 = ZonedDateTime.parse("2015-12-25 06:00:00", dateTimeFormatter);
        ZonedDateTime end1 = ZonedDateTime.parse("2015-12-25 07:00:00", dateTimeFormatter);
        ZonedDateTime start2 = ZonedDateTime.parse("2015-12-25 15:00:00", dateTimeFormatter);
        ZonedDateTime end2 = ZonedDateTime.parse("2015-12-25 16:00:00", dateTimeFormatter);
        testTimeLine1 = new TimeLine(start1, end1);
        testTimeLine2 = new TimeLine(start2, end2);
        timeLineList.add(testTimeLine1);
        timeLineList.add(testTimeLine2);
        temporalIntersectCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
        temporalContainCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.CONTAIN);
    }

    public void testExecuteRDDIntersectQuery() throws IOException {
        Database instance = Database.getInstance();
        IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
        TemporalQuery temporalQuery =
                new TemporalQuery(instance.getDataSet(DATASET_NAME), temporalIntersectCondition);
        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = temporalQuery.getRDDQuery(sparkSession);
            List<Trajectory> results = rddQuery.collect();
            System.out.println(results.size());
            for (Trajectory result : results) {
                ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
                System.out.println(
                        indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
                ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
                ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
                System.out.println(new TimeLine(startTime, endTime));
            }
            assertEquals(
                    13, results.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void testINTERSECTQuery() throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(DATASET_NAME);
        TemporalQuery temporalQuery = new TemporalQuery(dataSet, temporalIntersectCondition);
        List<Trajectory> trajectories = temporalQuery.executeQuery();
        System.out.println(trajectories.size());
        for (Trajectory trajectory : trajectories) {
            System.out.println(trajectory);
        }
        assertEquals(13, trajectories.size());
    }

    public void testContainQuery() throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(DATASET_NAME);
        TemporalQuery temporalQuery = new TemporalQuery(dataSet, temporalContainCondition);
        List<Trajectory> trajectories = temporalQuery.executeQuery();
        System.out.println(trajectories.size());
        for (Trajectory trajectory : trajectories) {
            System.out.println(trajectory);
        }
        assertEquals(10, trajectories.size());
    }

    public void testGetAnswer() throws IOException {
        JavaRDD<Trajectory> loadHBase = getLoadHBase();
        List<Trajectory> trips = loadHBase.collect();
        int i = 0;
        int j = 0;
        for (Trajectory trajectory : trips) {
            ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
            ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
            TimeLine trajTimeLine = new TimeLine(startTime, endTime);
            for (TimeLine queryTimeLine : timeLineList) {
                if (queryTimeLine.contain(trajTimeLine)) {
                    System.out.println(new TimeLine(startTime, endTime));
                    i++;
                }
                if (queryTimeLine.intersect(trajTimeLine)) {
                    j++;
                }
            }
        }
        System.out.println("CONTAIN: " + i);
        System.out.println("INTERSECT: " + j);
    }

    public void testDeleteDataSet() throws IOException {
        Database instance = Database.getInstance();
        instance.deleteDataSet(DATASET_NAME);
    }
}
