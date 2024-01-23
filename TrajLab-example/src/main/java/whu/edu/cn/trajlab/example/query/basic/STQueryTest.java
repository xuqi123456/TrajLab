package whu.edu.cn.trajlab.example.query.basic;

import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
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
import whu.edu.cn.trajlab.query.query.basic.SpatialTemporalQuery;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class STQueryTest extends TestCase {
    static SpatialTemporalQueryCondition stQueryConditionContain;
    static SpatialTemporalQueryCondition stQueryConditionIntersect;
    static String QUERY_WKT_INTERSECT =
            "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
    static String QUERY_WKT_CONTAIN =
            "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";
    static List<TimeLine> timeLineList = IDTemporalQueryTest.timeLineList;

    static {
        // 查询条件
        TemporalQueryCondition temporalContainQuery = IDTemporalQueryTest.temporalContainCondition;
        TemporalQueryCondition temporalIntersectQuery = IDTemporalQueryTest.temporalIntersectCondition;
        SpatialQueryCondition spatialIntersectQueryCondition = SpatialQueryTest.spatialIntersectQueryCondition;
        SpatialQueryCondition spatialContainedQueryCondition = SpatialQueryTest.spatialContainedQueryCondition;
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
        assertEquals(trajectories.size(),1);
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
        assertEquals(trajectories.size(),8);
    }

    public void testDeleteDataSet() throws IOException {
        Database instance = Database.getInstance();
        instance.deleteDataSet(DATASET_NAME);
    }

    public void testGetAnswer() throws IOException, ParseException {
        JavaRDD<Trajectory> loadHBase = getLoadHBase();
        List<Trajectory> trips = loadHBase.collect();
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
                        System.out.println(new TimeLine(startTime, endTime));
                        i++;
                    }
                }
            }
        }
        System.out.println("CONTAIN: " + i);
        for (Trajectory trajectory : trips) {
            ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
            ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
            TimeLine trajTimeLine = new TimeLine(startTime, endTime);
            if (intersectEnv.intersects(trajectory.getLineString())) {
                for (TimeLine timeLine : timeLineList) {
                    if (timeLine.intersect(trajTimeLine)) {
                        System.out.println(new TimeLine(startTime, endTime));
                        j++;
                    }
                }
            }
        }
        System.out.println("INTERSECT: " + j);
    }
}
