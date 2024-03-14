package whu.edu.cn.trajlab.example.query.basic;

import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.condition.SpatialTemporalQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.basic.SpatialQuery;
import whu.edu.cn.trajlab.query.query.basic.SpatialTemporalQuery;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.*;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class STQueryTest extends TestCase {
    static SpatialTemporalQueryCondition stQueryConditionContain;
    static SpatialTemporalQueryCondition stQueryConditionIntersect;
    static List<TimeLine> timeLineList = IDTemporalQueryTest.timeLineList;

    static {
        // 查询条件
        TemporalQueryCondition temporalContainQuery = IDTemporalQueryTest.temporalContainCondition;
        TemporalQueryCondition temporalIntersectQuery = IDTemporalQueryTest.temporalIntersectCondition;
        SpatialQueryCondition spatialIntersectQueryCondition = SpatialQueryTest.spatialIntersectQueryCondition;
        SpatialQueryCondition spatialContainedQueryCondition = SpatialQueryTest.spatialContainedMinQueryCondition;
        stQueryConditionIntersect = new SpatialTemporalQueryCondition(
                spatialIntersectQueryCondition, temporalIntersectQuery);
        stQueryConditionContain = new SpatialTemporalQueryCondition(
                spatialContainedQueryCondition, temporalContainQuery);
    }

//    public void testAddCoprocessor() throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        String tableName = DATASET_NAME + DBConstants.DATA_TABLE_SUFFIX;
//        String className = STQueryEndPoint.class.getCanonicalName();
//        String jarPath = "hdfs://localhost:9000/coprocessor/trajspark-db-1.0-SNAPSHOT.jar";
//        addCoprocessor(conf, tableName, className, jarPath);
//    }

//    public void testDeleteCoprocessor() throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        String tableName = DATASET_NAME + DBConstants.DATA_TABLE_SUFFIX;
//        deleteCoprocessor(conf, tableName);
//    }

    public void testINTERSECQuery() throws IOException, ParseException {
        Database instance = Database.getInstance();
        IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
        SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(DATASET_NAME),
                stQueryConditionIntersect);
        List<Trajectory> trajectories = spatialTemporalQuery.executeQuery();
        System.out.println(trajectories.size());
        WKTReader wktReader = new WKTReader();
        for (Trajectory trajectory : trajectories) {
            ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
            ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
            System.out.println(new TimeLine(startTime, endTime));
            System.out.println(indexTable.getIndexMeta().getIndexStrategy()
                    .index(trajectory));
            Polygon envelopeINTERSECT = (Polygon) wktReader.read(QUERY_WKT_INTERSECT).getEnvelope();
            System.out.println("envelopeINTERSECT :  " + envelopeINTERSECT.intersects(trajectory.getLineString()));
        }
        int answer = testGetAnswer(false);
        assertEquals(trajectories.size(),answer);
    }
    public void testExecuteRDDIntersectQuery() throws IOException {
        long start = System.currentTimeMillis();
        Database instance = Database.getInstance();
        IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
        SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(DATASET_NAME),
                stQueryConditionIntersect);
        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = spatialTemporalQuery.getRDDQuery(sparkSession);
            List<Trajectory> results = rddQuery.collect();
            System.out.println(results.size());
            long end = System.currentTimeMillis();
            System.out.println("cost : " + (end - start) + "ms");
//      for (Trajectory result : results) {
//        ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
//        System.out.println(
//            indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
//        ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
//        ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
//        System.out.println(new TimeLine(startTime, endTime));
//      }
            int answer = testGetAnswer(false);
            assertEquals(
                    answer, results.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testContainQuery() throws IOException, ParseException {
        Database instance = Database.getInstance();
        IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
        SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(DATASET_NAME),
                stQueryConditionContain);
        List<Trajectory> trajectories = spatialTemporalQuery.executeQuery();
        System.out.println(trajectories.size());
        for (Trajectory trajectory : trajectories) {
            ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
            ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
            System.out.println(new TimeLine(startTime, endTime));
            System.out.println(indexTable.getIndexMeta().getIndexStrategy()
                    .index(trajectory));
            WKTReader wktReader = new WKTReader();
            Polygon envelopeCONTAIN = (Polygon) wktReader.read(QUERY_WKT_CONTAIN).getEnvelope();
            System.out.println("envelopeCONTAIN :  " + envelopeCONTAIN.contains(trajectory.getLineString()));
        }
        int answer = testGetAnswer(true);
        assertEquals(trajectories.size(),answer);
    }

    public void testDeleteDataSet() throws IOException {
        Database instance = Database.getInstance();
        instance.deleteDataSet(DATASET_NAME);
    }

    public int testGetAnswer(boolean isContained) throws IOException, ParseException {
        List<Trajectory> trips = getLoadHBase();
        int i = 0;
        int j = 0;
        WKTReader wktReader = new WKTReader();
        Polygon containEnv = (Polygon) wktReader.read(QUERY_WKT_CONTAIN).getEnvelope();
        Polygon intersectEnv = (Polygon) wktReader.read(QUERY_WKT_INTERSECT).getEnvelope();
        for (Trajectory trajectory : trips) {
            ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
            ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
            TimeLine trajTimeLine = new TimeLine(startTime, endTime);
            if (containEnv.contains(trajectory.getLineString())) {
                for (TimeLine timeLine : timeLineList) {
                    if (timeLine.contain(trajTimeLine)) {
//                        System.out.println(new TimeLine(startTime, endTime));
                        i++;
                    }
                }
            }
        }
        for (Trajectory trajectory : trips) {
            ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
            ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
            TimeLine trajTimeLine = new TimeLine(startTime, endTime);
            if (intersectEnv.intersects(trajectory.getLineString())) {
                for (TimeLine timeLine : timeLineList) {
                    if (timeLine.intersect(trajTimeLine)) {
//                        System.out.println(new TimeLine(startTime, endTime));
                        j++;
                    }
                }
            }
        }
        if(isContained) return i;
        else return j;
    }
}
